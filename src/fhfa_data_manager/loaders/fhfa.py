"""FHFA (Federal Housing Finance Agency) data loader."""

from __future__ import annotations

import logging
import os
import re
import shutil
import zipfile
from pathlib import Path
from typing import Iterable, Literal, Optional

import polars as pl

from fhfa_data_manager.config import ImportOptions, PathsConfig, ensure_parent_dir
from fhfa_data_manager.dictionary import (
    extract_dictionary_tables_for_year,
    infer_year_from_name,
    resolve_dictionary_for_data_file,
)
from fhfa_data_manager.download import download_from_page
from fhfa_data_manager.fixed_width import load_fixed_width_dataset

logger = logging.getLogger(__name__)


class FHFADataLoader:
    """Loader encapsulating FHFA-related ETL steps."""

    def __init__(self, paths: Optional[PathsConfig] = None, options: Optional[ImportOptions] = None) -> None:
        self.paths = paths or PathsConfig.from_env()
        self.options = options or ImportOptions()

    # -------------------------------------------------
    # Dictionary resolution
    # -------------------------------------------------
    def resolve_dictionary_for_data_file(
        self,
        data_file: Path,
        *,
        dictionary_root: Optional[Path] = None,
        prefer_format: Literal['csv', 'parquet'] = 'parquet',
    ) -> Optional[Path]:
        """Return the matching parsed dictionary path for a given FHFA raw data file.

        Rules inferred from file naming conventions:
        - Prefix indicates enterprise: ``fnma_`` or ``fhlmc_`` (ignored for dictionary selection)
        - Segment ``sf`` vs ``mf`` distinguishes Single Family vs Multifamily
        - Letter following the 4-digit year indicates file type:
          - For Single Family: ``a``, ``b``, ``c``, ``d``
            - ``c`` aligns with "Single_Family_Census_Tract_File_C"
            - ``d`` aligns with "Single_Family_National_File_C"
            - ``a`` -> "Single_Family_National_File_A"
            - ``b`` -> "Single_Family_National_File_B"
          - For Multifamily: ``b`` (Units/Property-level in PUDB), ``c`` (Census tract)
            - We map both MF ``b`` variants to the available MF National File B dictionaries
              (Property-Level and Unit Class-Level). Preference order is Property-Level.

        The function looks in ``dictionary_files/<year>`` for a file named
        ``<year>_<Family>_<Descriptor>_combined.(csv|parquet)`` and returns the preferred
        format if present, else the other. If nothing matches, returns ``None``.
        """
        return resolve_dictionary_for_data_file(
            data_file,
            paths_project_dir=self.paths.project_dir,
            dictionary_root=dictionary_root,
            prefer_format=prefer_format,
        )

    def resolve_dictionaries_for_year_folder(
        self,
        year: int,
        *,
        raw_fhfa_root: Optional[Path] = None,
        dictionary_root: Optional[Path] = None,
        prefer_format: Literal['csv', 'parquet'] = 'parquet',
    ) -> dict[Path, Optional[Path]]:
        """Map each raw FHFA data file in ``data/raw/fhfa/<year>`` to its dictionary path.

        Returns a mapping of data file path -> dictionary path (or None if no match).
        """
        raw_root = raw_fhfa_root if raw_fhfa_root is not None else (self.paths.raw_dir / 'fhfa')
        year_dir = Path(raw_root) / str(year)
        if not year_dir.exists() or not year_dir.is_dir():
            return {}

        txt_files = [p for p in year_dir.iterdir() if p.is_file() and p.suffix.lower() == '.txt']
        result: dict[Path, Optional[Path]] = {}
        for data_path in txt_files:
            result[data_path] = self.resolve_dictionary_for_data_file(
                data_path,
                dictionary_root=dictionary_root,
                prefer_format=prefer_format,
            )
        return result

    # Download FHFA artifacts (zips and optional PDFs)
    def download(
        self,
        base_url: str,
        fhfa_zip_dir: Path,
        include_substrings: Optional[Iterable[str]] = ('pudb',),
        pause_seconds: Optional[int] = None,
    ) -> None:
        """Download FHFA-provided zip files (and optionally dictionaries as PDFs).

        The FHFA page uses mixed link labels; filter by extension and optional substrings.
        """
        ps = pause_seconds if pause_seconds is not None else self.options.pause_length_seconds
        # FHFA single-family datasets (zips)
        download_from_page(base_url, fhfa_zip_dir, allowed_extensions=['.zip'], included_substrings=include_substrings, pause_seconds=ps)

    def download_dictionaries(
        self,
        base_url: str,
        dictionary_dir: Path,
        pause_seconds: Optional[int] = None,
    ) -> None:
        ps = pause_seconds if pause_seconds is not None else self.options.pause_length_seconds
        download_from_page(
            base_url,
            dictionary_dir,
            allowed_extensions=['.pdf'],
            included_substrings=None,
            pause_seconds=ps,
            overwrite=self.options.overwrite_raw_dicts,
        )

    def extract_zip_contents(
        self,
        zip_dir: Path,
        dictionary_dir: Path,
        raw_base_dir: Optional[Path] = None,
    ) -> None:
        """Extract contents of all zip files in ``zip_dir``.

        - .pdf files are saved into ``dictionary_dir/<year>`` (flattened)
        - .txt files are saved into ``<raw_base_dir>/fhfa/<year>`` (defaults to ``self.paths.raw_dir``)

        The year is inferred primarily from the zip file name; if not found,
        falls back to the inner member name. If no year is found, files are
        If no year is found, files are placed under an ``unknown`` subfolder.
        Honors ``self.options.overwrite``.
        """
        raw_root = raw_base_dir or self.paths.raw_dir
        dictionary_dir.mkdir(parents=True, exist_ok=True)
        raw_root.mkdir(parents=True, exist_ok=True)
        raw_fhfa_root = raw_root / 'fhfa'
        raw_fhfa_root.mkdir(parents=True, exist_ok=True)

        # Collect zip files (case-insensitive) in the provided directory
        zip_paths: list[Path] = [p for p in Path(zip_dir).iterdir() if p.is_file() and p.suffix.lower() == '.zip']
        for zip_path in zip_paths:
            try:
                with zipfile.ZipFile(zip_path) as zf:
                    zip_year = infer_year_from_name(zip_path.name)
                    for member in zf.namelist():
                        # Skip directories
                        if member.endswith('/'):
                            continue

                        _, ext = os.path.splitext(member)
                        ext_lower = ext.lower()
                        base_name = os.path.basename(member)

                        # Route PDFs to dictionary_dir/<year>
                        if ext_lower == '.pdf':
                            pdf_year = zip_year or infer_year_from_name(member)
                            pdf_folder = 'unknown' if pdf_year is None else str(pdf_year)
                            out_dir = dictionary_dir / pdf_folder
                            out_path = out_dir / base_name
                            if out_path.exists() and not self.options.overwrite_raw_dicts:
                                continue
                            out_dir.mkdir(parents=True, exist_ok=True)
                            with zf.open(member) as src, open(out_path, 'wb') as dst:
                                shutil.copyfileobj(src, dst)
                            continue

                        # Route TXT files to raw/fhfa/<year> subfolders
                        if ext_lower == '.txt':
                            year = zip_year or infer_year_from_name(member)
                            year_folder = 'unknown' if year is None else str(year)
                            out_dir = raw_fhfa_root / year_folder
                            out_path = out_dir / base_name
                            if out_path.exists() and not self.options.overwrite:
                                continue
                            out_dir.mkdir(parents=True, exist_ok=True)
                            with zf.open(member) as src, open(out_path, 'wb') as dst:
                                shutil.copyfileobj(src, dst)
                            continue
            except zipfile.BadZipFile:
                logger.warning('Bad zip file encountered and skipped: %s', zip_path)
            except Exception as e:
                logger.error('Failed extracting from %s: %s', zip_path, e)

    def extract_dictionary_tables_for_year(
        self,
        year: int,
        dictionary_root: Optional[Path] = None,
        output_formats: Iterable[Literal['csv', 'parquet']] = ('csv', 'parquet'),
        table_settings: Optional[dict] = None,
    ) -> None:
        """Extract and combine tables per PDF in ``dictionary_files/<year>`` and save one file per PDF.

        Parameters
        ----------
        year: int
            Year subfolder under the dictionaries root.
        dictionary_root: Optional[pathlib.Path]
            Root directory that contains year subfolders; defaults to ``<project_dir>/dictionary_files``.
        output_formats: Iterable['csv' | 'parquet']
            One or both of 'csv' and 'parquet'. Defaults to both. Combined per PDF.
        table_settings: Optional[dict]
            Optional settings for PyMuPDF's table detection (currently not used).

        Behavior
        --------
        - Iterates all ``*.pdf`` files under ``dictionary_root/<year>`` (non-recursive).
        - Extracts all tables from every page using PyMuPDF's ``page.find_tables()``.
        - Retains only tables containing a case/spacing-insensitive "Field #" column.
        - Combines tables per document, standardizes the column to ``Field #``, coerces to int, sorts by it,
          and drops duplicate field numbers keeping the first occurrence.
        - Writes exactly one combined output per PDF next to the source PDF as:
            ``<pdf_stem>_combined.csv`` and/or ``.parquet``.
        - Prints a warning if there are gaps in the numbering between min and max ``Field #``.
        - Honors ``self.options.overwrite``. If both outputs already exist and overwrite=False, skips.
        """
        extract_dictionary_tables_for_year(
            year=year,
            paths_project_dir=self.paths.project_dir,
            options_overwrite_clean_dicts=self.options.overwrite_clean_dicts,
            dictionary_root=dictionary_root,
            output_formats=output_formats,
            table_settings=table_settings,
        )

    def extract_dictionary_tables_all_years(
        self,
        dictionary_root: Optional[Path] = None,
        output_formats: Iterable[Literal['csv', 'parquet']] = ('csv', 'parquet'),
        table_settings: Optional[dict] = None,
        years: Optional[Iterable[int]] = None,
        min_year: Optional[int] = None,
        max_year: Optional[int] = None,
    ) -> None:
        """Parse and combine dictionary tables for all year subfolders under the dictionaries root.

        Parameters
        ----------
        dictionary_root: Optional[pathlib.Path]
            Root directory that contains year subfolders; defaults to ``<project_dir>/dictionary_files``.
        output_formats: Iterable['csv' | 'parquet']
            Output format(s) to write per PDF (combined). Defaults to both.
        table_settings: Optional[dict]
            Optional settings for PyMuPDF table detection (reserved).
        years: Optional[Iterable[int]]
            Explicit list of years to process. If None, infer from subfolder names.
        min_year, max_year: Optional[int]
            When inferring years from subfolders, restrict to this inclusive range if provided.
        """
        dict_root = dictionary_root if dictionary_root is not None else (self.paths.project_dir / 'dictionary_files')
        root = Path(dict_root)
        if not root.exists() or not root.is_dir():
            logger.warning('Dictionary root does not exist: %s', root)
            return

        if years is not None:
            year_list = sorted({int(y) for y in years})
        else:
            candidates = [p for p in root.iterdir() if p.is_dir()]
            inferred: list[int] = []
            for p in candidates:
                try:
                    y = int(p.name)
                except Exception:
                    continue
                if 1990 <= y <= 2035:
                    inferred.append(y)
            year_list = sorted(set(inferred))

        if min_year is not None or max_year is not None:
            lo = min_year if min_year is not None else -10**9
            hi = max_year if max_year is not None else 10**9
            year_list = [y for y in year_list if lo <= y <= hi]

        for y in year_list:
            logger.info('Processing dictionary tables for year: %s', y)
            self.extract_dictionary_tables_for_year(
                year=y,
                dictionary_root=root,
                output_formats=output_formats,
                table_settings=table_settings,
            )

    def load_fixed_width_dataset(
        self,
        data_path: Path,
        dictionary_path: Path,
        *,
        encoding: str = 'latin-1',
        dictionary_format: Optional[Literal['csv', 'parquet']] = None,
        column_name_column: str = 'Field Name',
        width_column: str = 'Field Width',
        separator_width: int = 1,
    ) -> pl.DataFrame:
        """Load a fixed-width FHFA text file using a parsed data dictionary.

        Parameters
        ----------
        data_path:
            Path to the fixed width text file (no header row).
        dictionary_path:
            Path to the parsed dictionary (CSV or Parquet) that includes field
            names and widths. Typically produced by
            :meth:`extract_dictionary_tables_for_year`.
        encoding:
            Text encoding for the fixed width file. Defaults to ``'latin-1'`` as
            FHFA releases often use extended ASCII.
        dictionary_format:
            Optional override for the dictionary format. If ``None`` the format
            is inferred from ``dictionary_path`` suffix.
        column_name_column:
            Column in the dictionary file that contains the desired column
            labels. Defaults to ``'Field Name'``.
        width_column:
            Column in the dictionary file that contains field widths. Defaults
            to ``'Field Width'``.

        Returns
        -------
        polars.DataFrame
            Dataset with columns named according to the dictionary.
        """
        return load_fixed_width_dataset(
            data_path=data_path,
            dictionary_path=dictionary_path,
            encoding=encoding,
            dictionary_format=dictionary_format,
            column_name_column=column_name_column,
            width_column=width_column,
            separator_width=separator_width,
        )

    def convert_all_fixed_width_to_parquet(
        self,
        *,
        raw_fhfa_root: Optional[Path] = None,
        dictionary_root: Optional[Path] = None,
        output_root: Optional[Path] = None,
        years: Optional[Iterable[int]] = None,
        min_year: Optional[int] = None,
        max_year: Optional[int] = None,
        encoding: str = 'latin-1',
        separator_width: int = 1,
        prefer_format: Literal['csv', 'parquet'] = 'parquet',
    ) -> list[tuple[Path, Path]]:
        """Parse all fixed-width FHFA ``.txt`` files and save as Parquet in clean folder.

        Behavior
        --------
        - Scans ``<raw_dir>/fhfa/<year>`` for ``*.txt`` files (or ``raw_fhfa_root`` if provided)
        - Resolves the appropriate parsed dictionary for each file
        - Loads the fixed-width file using ``load_fixed_width_dataset``
        - Writes a Parquet with the same base filename under ``<clean_dir>/fhfa/<year>``
          (or ``output_root`` if provided)
        - Skips existing outputs unless ``self.options.overwrite`` is True

        Returns
        -------
        list[tuple[pathlib.Path, pathlib.Path]]
            List of (input_path, output_parquet_path) successfully written.
        """
        raw_root = raw_fhfa_root if raw_fhfa_root is not None else (self.paths.raw_dir / 'fhfa')
        out_root = output_root if output_root is not None else (self.paths.clean_dir / 'fhfa')
        dict_root = dictionary_root if dictionary_root is not None else (self.paths.project_dir / 'dictionary_files')

        if not Path(raw_root).exists():
            logger.warning('Raw FHFA root does not exist: %s', raw_root)
            return []

        # Determine year folders
        if years is not None:
            year_list = sorted({int(y) for y in years})
        else:
            candidates = [p for p in Path(raw_root).iterdir() if p.is_dir()]
            inferred: list[int] = []
            for p in candidates:
                try:
                    y = int(p.name)
                except Exception:
                    continue
                if 1990 <= y <= 2035:
                    inferred.append(y)
            year_list = sorted(set(inferred))

        if min_year is not None or max_year is not None:
            lo = min_year if min_year is not None else -10**9
            hi = max_year if max_year is not None else 10**9
            year_list = [y for y in year_list if lo <= y <= hi]

        written: list[tuple[Path, Path]] = []

        for year in year_list:
            year_dir = Path(raw_root) / str(year)
            if not year_dir.exists() or not year_dir.is_dir():
                continue

            txt_files = [p for p in year_dir.iterdir() if p.is_file() and p.suffix.lower() == '.txt']
            for data_path in txt_files:
                try:
                    dict_path = self.resolve_dictionary_for_data_file(
                        data_path,
                        dictionary_root=dict_root,
                        prefer_format=prefer_format,
                    )
                except Exception as e:
                    logger.warning('Failed resolving dictionary for %s: %s', data_path.name, e)
                    continue

                if dict_path is None:
                    logger.warning('No dictionary found for %s; skipping.', data_path.name)
                    continue

                out_dir = Path(out_root) / str(year)
                out_path = out_dir / (data_path.stem + '.parquet')
                if out_path.exists() and not self.options.overwrite:
                    # Already converted; skip
                    continue

                try:
                    df = self.load_fixed_width_dataset(
                        data_path=data_path,
                        dictionary_path=dict_path,
                        encoding=encoding,
                        separator_width=separator_width,
                        dictionary_format=None,
                    )
                except Exception as e:
                    logger.error('Failed loading fixed-width for %s: %s', data_path.name, e)
                    continue

                try:
                    ensure_parent_dir(out_path)
                    df.write_parquet(str(out_path))
                    written.append((data_path, out_path))
                except Exception as e:
                    logger.error('Failed writing Parquet for %s: %s', data_path.name, e)
                    continue

        return written

    def combine_parquet_by_type(
        self,
        *,
        file_type_pattern: str,
        enterprises: Iterable[Literal['fnma', 'fhlmc']] = ('fnma', 'fhlmc'),
        years: Optional[Iterable[int]] = None,
        min_year: Optional[int] = None,
        max_year: Optional[int] = None,
        input_root: Optional[Path] = None,
        output_path: Optional[Path] = None,
        write_mode: Literal['parquet', 'csv'] = 'parquet',
    ) -> Path:
        """Combine clean FHFA Parquet files across years for a given type.

        Parameters
        ----------
        file_type_pattern:
            Base stem pattern that identifies the dataset type, e.g. ``'sf{year}c'`` or ``'mf{year}b_loans'``.
            The literal substring ``"{year}"`` will be replaced by 4-digit years when scanning.
        enterprises:
            Which enterprises to include. Any subset of {"fnma", "fhlmc"}.
        years:
            Explicit list of years to include. If None, infer from directory names under the clean root.
        min_year, max_year:
            Optional inclusive range filter when inferring years.
        input_root:
            Override for the clean FHFA root. Defaults to ``self.paths.clean_dir / 'fhfa'``.
        output_path:
            Optional explicit output file path. If not provided, writes to
            ``<clean_dir>/fhfa/<file_type_pattern_without_braces>_<min>-<max>_<enterprises>.parquet``.
        write_mode:
            Output format: "parquet" (default) or "csv". Parquet is recommended.

        Returns
        -------
        pathlib.Path
            Path to the written combined dataset.
        """
        clean_root = Path(input_root) if input_root is not None else (self.paths.clean_dir / 'fhfa')

        # Determine candidate years
        if years is not None:
            year_list = sorted({int(y) for y in years})
        else:
            candidates = [p for p in Path(clean_root).iterdir() if p.is_dir()]
            inferred: list[int] = []
            for p in candidates:
                try:
                    y = int(p.name)
                except Exception:
                    continue
                if 1990 <= y <= 2035:
                    inferred.append(y)
            year_list = sorted(set(inferred))

        if min_year is not None or max_year is not None:
            lo = min_year if min_year is not None else 2008
            hi = max_year if max_year is not None else 2035
            year_list = [y for y in year_list if lo <= y <= hi]

        ent_set = {e.lower() for e in enterprises}
        if not ent_set.issubset({'fnma', 'fhlmc'}):
            raise ValueError('enterprises must be any subset of {"fnma", "fhlmc"}')

        # Build list of existing files to scan
        parquet_paths: list[Path] = []
        for y in year_list:
            year_dir = Path(clean_root) / str(y)
            if not year_dir.exists():
                continue
            # Construct expected stems for each enterprise for this year
            stem = file_type_pattern.replace('{year}', str(y))
            for ent in sorted(ent_set):
                # Expected filename begins with e.g. fnma_sf2019c or fhlmc_mf2019b_loans
                glob_pattern = f"{ent}_{stem}.parquet"
                matches = list(year_dir.glob(glob_pattern))
                parquet_paths.extend(matches)

        if not parquet_paths:
            raise FileNotFoundError(
                f'No Parquet files found for pattern={file_type_pattern!r}, years={year_list}, enterprises={sorted(ent_set)} under {clean_root}'
            )

        # Prefer Polars lazy scan, then concatenate lazily
        lazy_frames: list[pl.LazyFrame] = []
        for p in parquet_paths:
            try:
                lf = pl.scan_parquet(p)
                # Attach origin metadata as columns
                year_match = infer_year_from_name(p.name) or infer_year_from_name(p.as_posix())
                enterprise_val = 'fnma' if 'fnma_' in p.name.lower() else ('fhlmc' if 'fhlmc_' in p.name.lower() else None)
                annotated = lf.with_columns([
                    pl.lit(year_match).alias('Year'),
                    pl.lit(enterprise_val).alias('Enterprise'),
                ])
                lazy_frames.append(annotated)
            except Exception as e:
                logger.warning('Failed to scan %s: %s', p, e)

        if not lazy_frames:
            raise RuntimeError('No readable Parquet files after scanning candidates.')

        combined = pl.concat(lazy_frames, how='diagonal_relaxed')

        # Determine output path
        if output_path is not None:
            out_path = Path(output_path)
        else:
            years_min = min(year_list) if year_list else None
            years_max = max(year_list) if year_list else None
            yrs_label = f"{years_min}-{years_max}" if years_min is not None and years_max is not None else "all"
            ent_label = "-".join(sorted(ent_set)) if ent_set else "both"
            # Create a safe label for the type pattern
            type_label = file_type_pattern.replace('{year}', 'YYYY')
            type_label = re.sub(r'[^A-Za-z0-9_\-]', '_', type_label)
            out_dir = clean_root / 'combined'
            out_dir.mkdir(parents=True, exist_ok=True)
            suffix = '.parquet' if write_mode == 'parquet' else '.csv'
            out_path = out_dir / f"{type_label}_{yrs_label}_{ent_label}{suffix}"

        # Write output
        if write_mode == 'parquet':
            combined.sink_parquet(str(out_path))
        elif write_mode == 'csv':
            combined.sink_csv(str(out_path))
        else:
            raise ValueError('write_mode must be either "parquet" or "csv"')

        return out_path

