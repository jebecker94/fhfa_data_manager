# -*- coding: utf-8 -*-
"""
Unified configuration and dataset loader classes for FHFA and FHLB pipelines.

This module introduces:
- PathsConfig and ImportOptions dataclasses for typed, centralized configuration
- FHFADataLoader: methods wrapping FHFA-related ETL steps
- FHLBDataLoader: methods wrapping FHLB-related ETL steps

Existing functional scripts can continue to work; these classes provide a
structured, testable API for orchestration and future unification.
"""

# Standard Library
from __future__ import annotations
import os
import glob
import zipfile
import subprocess
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Iterable, Literal, Sequence
import datetime
import time
from urllib.parse import urljoin, urlparse

# Third-party
import pandas as pd
import numpy as np
import pyodbc
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import csv as pacsv
from dateutil.relativedelta import relativedelta
import requests
from bs4 import BeautifulSoup
from bs4.element import Tag
import fitz  # PyMuPDF
import polars as pl


# -----------------------------
# Configuration Dataclasses
# -----------------------------

@dataclass(frozen=True)
class PathsConfig:
    """Holds normalized path configuration.

    All paths are guaranteed to be pathlib.Path instances.
    """

    project_dir: Path
    data_dir: Path
    raw_dir: Path
    clean_dir: Path

    @staticmethod
    def _to_path(value: object) -> Path:
        if isinstance(value, Path):
            return value
        if value is None:
            return Path.cwd()
        return Path(str(value))

    @classmethod
    def from_env(cls,
                 project_env: str = 'PROJECT_DIR',
                 data_env: str = 'DATA_DIR',
                 raw_env: str = 'FHFA_RAW_DIR',
                 clean_env: str = 'FHFA_CLEAN_DIR',
                 ) -> 'PathsConfig':
        """Build paths from environment variables with sensible defaults."""
        # Try to use python-decouple if available, else fallback to os.environ
        def get_env(name: str, default: Optional[str]) -> Optional[str]:
            try:
                from decouple import config as dconfig  # type: ignore
                v = dconfig(name, default=default)
            except Exception:
                v = os.environ.get(name, default)
            return None if v is None else str(v)

        project_dir = cls._to_path(get_env(project_env, str(Path.cwd())))
        data_dir = cls._to_path(get_env(data_env, str(project_dir / 'data')))
        raw_dir = cls._to_path(get_env(raw_env, str(data_dir / 'raw')))
        clean_dir = cls._to_path(get_env(clean_env, str(data_dir / 'clean')))
        return cls(project_dir=project_dir, data_dir=data_dir, raw_dir=raw_dir, clean_dir=clean_dir)


@dataclass(frozen=True)
class ImportOptions:
    """Common operational options for data loaders."""
    overwrite: bool = False
    # Separate overwrite flags for dictionary artifacts (no fallback)
    # - raw dictionaries: PDFs scraped or extracted from zips
    # - clean dictionaries: parsed/combined CSV/Parquet next to PDFs
    overwrite_raw_dicts: bool = False
    overwrite_clean_dicts: bool = False
    pause_length_seconds: int = 5
    excel_engine: Optional[Literal['xlrd', 'openpyxl', 'odf', 'pyxlsb', 'calamine']] = 'openpyxl'


# -----------------------------
# Helper utilities
# -----------------------------

def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def drop_unnamed_columns(df: pd.DataFrame) -> pd.DataFrame:
    unnamed_cols = [c for c in df.columns if isinstance(c, str) and 'unnamed' in c.lower()]
    if unnamed_cols:
        return df.drop(columns=unnamed_cols, errors='ignore')
    return df


def _download_from_page(
    base_url: str,
    download_dir: Path,
    allowed_extensions: Iterable[str],
    included_substrings: Optional[Iterable[str]] = None,
    pause_seconds: int = 5,
    *,
    overwrite: bool = False,
) -> None:
    """Scrape page for links and download files that match filters.

    - allowed_extensions: case-insensitive (e.g., ['.zip'])
    - included_substrings: optional case-insensitive filename substrings to include
    """
    download_dir.mkdir(parents=True, exist_ok=True)

    response = requests.get(base_url)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, 'html.parser')
    links = soup.find_all('a', href=True)

    allowed_lower = tuple(ext.lower() for ext in allowed_extensions)
    substrings_lower = tuple(s.lower() for s in included_substrings) if included_substrings else tuple()

    for link in links:
        if not isinstance(link, Tag):
            continue
        href_value = link.get('href')
        if not isinstance(href_value, str) or not href_value:
            continue
        file_url = urljoin(base_url, href_value)
        parsed = urlparse(file_url)
        file_name = os.path.basename(parsed.path)
        if not file_name:
            continue

        _, ext = os.path.splitext(file_name)
        if ext.lower() not in allowed_lower:
            continue

        if substrings_lower and all(s not in file_name.lower() for s in substrings_lower):
            continue

        file_path = download_dir / file_name
        if file_path.exists() and not overwrite:
            continue

        try:
            print(f'Downloading {file_url}...')
            file_response = requests.get(file_url)
            file_response.raise_for_status()
            ensure_parent_dir(file_path)
            with open(file_path, 'wb') as f:
                f.write(file_response.content)
            print(f'Saved to {file_path}')
            time.sleep(pause_seconds)
        except requests.RequestException as e:
            print(f'Failed to download {file_url}: {e}')


# -----------------------------
# FHFA Loader
# -----------------------------

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

        path = Path(data_file)
        year = self._infer_year_from_name(path.name)
        if year is None:
            return None

        dict_root = dictionary_root if dictionary_root is not None else (self.paths.project_dir / 'dictionary_files')
        year_dir = Path(dict_root) / str(year)
        if not year_dir.exists():
            return None

        name_lower = path.name.lower()
        is_single_family = '_sf' in name_lower or 'sf' in name_lower
        is_multifamily = '_mf' in name_lower or 'mf' in name_lower

        # Extract type letter immediately after year, e.g., sf2013c -> "c"
        match = re.search(r'(sf|mf)(?:19|20)\d{2}([a-d])', name_lower)
        letter = match.group(2) if match else None

        candidates: list[str] = []
        if is_single_family:
            if letter == 'a':
                candidates = [f'{year}_Single_Family_National_File_A_combined']
            elif letter == 'b':
                candidates = [f'{year}_Single_Family_National_File_B_combined']
            elif letter == 'c':
                # Census tract file
                candidates = [f'{year}_Single_Family_Census_Tract_File_C_combined']
            elif letter == 'd':
                # National File C (per user rule)
                candidates = [f'{year}_Single_Family_National_File_C_combined']
            else:
                # Fallback preference order for SF if letter not parsed
                candidates = [
                    f'{year}_Single_Family_National_File_C_combined',
                    f'{year}_Single_Family_Census_Tract_File_C_combined',
                    f'{year}_Single_Family_National_File_B_combined',
                    f'{year}_Single_Family_National_File_A_combined',
                ]
        elif is_multifamily:
            if letter == 'c':
                candidates = [f'{year}_Multifamily_Census_Tract_File_C_combined']
            else:
                # MF "b" has two flavors: loans (property-level) and units (unit class-level)
                # Disambiguate using filename tokens
                is_units = 'units' in name_lower
                is_loans = 'loans' in name_lower
                if is_units and not is_loans:
                    candidates = [
                        f'{year}_Multifamily_National_File_Unit_Class-Level_Data_File_B_combined',
                        f'{year}_Multifamily_National_File_Property-Level_Data_File_B_combined',
                    ]
                else:
                    # Default to loans/property-level if ambiguous
                    candidates = [
                        f'{year}_Multifamily_National_File_Property-Level_Data_File_B_combined',
                        f'{year}_Multifamily_National_File_Unit_Class-Level_Data_File_B_combined',
                    ]
        else:
            # Could not determine family; try SF then MF fallbacks
            candidates = [
                f'{year}_Single_Family_National_File_C_combined',
                f'{year}_Single_Family_Census_Tract_File_C_combined',
                f'{year}_Single_Family_National_File_B_combined',
                f'{year}_Single_Family_National_File_A_combined',
                f'{year}_Multifamily_National_File_Property-Level_Data_File_B_combined',
                f'{year}_Multifamily_National_File_Unit_Class-Level_Data_File_B_combined',
                f'{year}_Multifamily_Census_Tract_File_C_combined',
            ]

        # Try building full paths with preferred format, then the alternate
        def pick_existing(stem: str) -> Optional[Path]:
            first = year_dir / f'{stem}.{prefer_format}'
            if first.exists():
                return first
            alt = year_dir / f'{stem}.{"csv" if prefer_format == "parquet" else "parquet"}'
            if alt.exists():
                return alt
            return None

        for stem in candidates:
            found = pick_existing(stem)
            if found is not None:
                return found

        return None

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
        _download_from_page(base_url, fhfa_zip_dir, allowed_extensions=['.zip'], included_substrings=include_substrings, pause_seconds=ps)

    def download_dictionaries(
        self,
        base_url: str,
        dictionary_dir: Path,
        pause_seconds: Optional[int] = None,
    ) -> None:
        ps = pause_seconds if pause_seconds is not None else self.options.pause_length_seconds
        _download_from_page(
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
                    zip_year = self._infer_year_from_name(zip_path.name)
                    for member in zf.namelist():
                        # Skip directories
                        if member.endswith('/'):
                            continue

                        _, ext = os.path.splitext(member)
                        ext_lower = ext.lower()
                        base_name = os.path.basename(member)

                        # Route PDFs to dictionary_dir/<year>
                        if ext_lower == '.pdf':
                            pdf_year = zip_year or self._infer_year_from_name(member)
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
                            year = zip_year or self._infer_year_from_name(member)
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
                print(f'Bad zip file encountered and skipped: {zip_path}')
            except Exception as e:
                print(f'Failed extracting from {zip_path}: {e}')

    @staticmethod
    def _infer_year_from_name(name: str) -> Optional[int]:
        """Infer a plausible 4-digit year from a file or path name.

        Looks for years in [1990, 2035]. If multiple are present, returns the
        largest year (most recent), which aligns with common FHFA naming.
        """
        matches = re.findall(r'(?<!\d)(?:19|20)\d{2}(?!\d)', name)
        years = [int(y) for y in matches]
        years = [y for y in years if 1990 <= y <= 2035]
        if not years:
            return None
        return max(years)

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

        dict_root = dictionary_root if dictionary_root is not None else (self.paths.project_dir / 'dictionary_files')
        year_dir = Path(dict_root) / str(year)
        if not year_dir.exists() or not year_dir.is_dir():
            print(f'Dictionary directory does not exist: {year_dir}')
            return

        formats = set(output_formats)

        pdf_files = [p for p in year_dir.iterdir() if p.is_file() and p.suffix.lower() == '.pdf']
        for pdf_path in pdf_files:
            combined_frames: list[pl.DataFrame] = []
            try:
                with fitz.open(pdf_path) as doc:
                    for _page_zero_idx in range(doc.page_count):
                        page_index = _page_zero_idx + 1
                        page = doc.load_page(_page_zero_idx)
                        try:
                            _find_tables = getattr(page, 'find_tables', None)
                            tables_result = _find_tables() if callable(_find_tables) else None
                        except Exception as e:
                            print(f'find_tables failed on {pdf_path} page {page_index}: {e}')
                            continue

                        tables_list = getattr(tables_result, 'tables', None) if tables_result is not None else None
                        if not tables_list:
                            continue

                        for table in tables_list:
                            try:
                                pdf_df = table.to_pandas()
                            except Exception as e:
                                print(f'to_pandas failed on {pdf_path} p{page_index}: {e}')
                                continue

                            df = pl.from_pandas(pdf_df)

                            # Identify a "Field #" column (case/spacing-insensitive)
                            def normalize_label(label: str) -> str:
                                text = str(label).lower()
                                text = re.sub(r'[\s_]+', '', text)
                                return text

                            field_col: Optional[str] = None
                            for col in df.columns:
                                if normalize_label(col) == 'field#':
                                    field_col = str(col)
                                    break

                            if field_col is None:
                                continue

                            # Try to locate a field name column for aligned name expansion
                            name_col: Optional[str] = None
                            for col in df.columns:
                                norm_col = normalize_label(col)
                                if norm_col in ('fieldname', 'variablename', 'name'):
                                    name_col = str(col)
                                    break

                            # Standardize and expand single values, ranges (e.g., "19-23"), and comma-separated lists (e.g., "19, 21-23").
                            work = df.rename({field_col: 'Field #'})
                            # keep original as text and normalize unicode dashes to '-'
                            work = work.with_columns([
                                pl.col('Field #').cast(pl.Utf8, strict=False).alias('field_text')
                            ]).with_columns([
                                pl.col('field_text')
                                  .str.replace_all('–', '-')
                                  .str.replace_all('—', '-')
                                  .alias('field_text')
                            ])
                            # also capture variable name text if available and normalize dashes similarly
                            if name_col is not None:
                                work = work.with_columns([
                                    pl.col(name_col).cast(pl.Utf8, strict=False).alias('var_text')
                                ]).with_columns([
                                    pl.col('var_text')
                                      .str.replace_all('–', '-')
                                      .str.replace_all('—', '-')
                                      .alias('var_text')
                                ])
                            else:
                                work = work.with_columns([
                                    pl.lit(None).alias('var_text')
                                ])
                            # split on commas into tokens
                            work = work.with_columns([
                                pl.col('field_text').str.split(by=',').alias('field_tokens')
                            ]).explode('field_tokens').with_columns([
                                pl.col('field_tokens').map_elements(lambda s: s.strip() if isinstance(s, str) else s).alias('field_token')
                            ])
                            # parse start and optional end from each token
                            work = work.with_columns([
                                pl.col('field_token').str.extract(r'(\d+)', 1).cast(pl.Int64, strict=False).alias('field_start'),
                                pl.col('field_token').str.extract(r'\d+\s*-\s*(\d+)', 1).cast(pl.Int64, strict=False).alias('field_end'),
                            ])
                            # if no end, end = start, then build list range [start..end]
                            work = work.with_columns([
                                pl.coalesce([pl.col('field_end'), pl.col('field_start')]).alias('field_end2'),
                            ]).with_columns([
                                pl.int_ranges(pl.col('field_start'), pl.col('field_end2') + 1).alias('FieldNums')
                            ])
                            # explode and set Field # to each number
                            work = work.explode('FieldNums').with_columns([
                                pl.col('FieldNums').alias('Field #')
                            ])
                            # If a variable name with trailing range exists, compute aligned name numbers
                            if name_col is not None:
                                work = work.with_columns([
                                    # extract trailing name range and lengths
                                    pl.col('var_text').str.extract(r'(\d+)\s*-\s*(\d+)\s*$', 1).cast(pl.Int64, strict=False).alias('name_start'),
                                    pl.col('var_text').str.extract(r'(\d+)\s*-\s*(\d+)\s*$', 2).cast(pl.Int64, strict=False).alias('name_end'),
                                ])
                                work = work.with_columns([
                                    (pl.col('FieldNums') - pl.col('field_start')).alias('field_offset'),
                                    (pl.col('name_end') - pl.col('name_start')).alias('name_len'),
                                ]).with_columns([
                                    pl.when(
                                        pl.col('name_start').is_not_null()
                                        & pl.col('name_end').is_not_null()
                                        & (pl.col('name_len') == (pl.col('field_end2') - pl.col('field_start')))
                                    )
                                    .then(pl.col('name_start') + pl.col('field_offset'))
                                    .otherwise(None)
                                    .alias('NameNum')
                                ])
                                # build expanded name when NameNum is available
                                work = work.with_columns([
                                    # name_prefix by stripping trailing range
                                    pl.col('var_text')
                                      .str.replace(r'\s*\d+\s*-\s*\d+\s*$', '', literal=False)
                                      .map_elements(lambda s: s.strip() if isinstance(s, str) else s)
                                      .alias('name_prefix'),
                                ]).with_columns([
                                    pl.when(pl.col('NameNum').is_not_null())
                                      .then(pl.col('name_prefix') + pl.lit(' ') + pl.col('NameNum').cast(pl.Utf8))
                                      .otherwise(pl.col('var_text'))
                                      .alias(name_col)
                                ])
                            # Drop helper cols and rows with empty Field #
                            drop_cols = ['field_text', 'field_tokens', 'field_token', 'field_start', 'field_end', 'field_end2', 'FieldNums']
                            if name_col is not None:
                                drop_cols += ['var_text', 'name_start', 'name_end', 'field_offset', 'name_len', 'name_prefix']
                            work = work.drop(drop_cols)
                            work = work.filter(pl.col('Field #').is_not_null())

                            combined_frames.append(work)
            except Exception as e:
                print(f'Failed processing {pdf_path}: {e}')
                combined_frames = []

            if not combined_frames:
                continue

            combined = pl.concat(combined_frames, how='vertical', rechunk=True)
        # Drop rows without field number, drop dupes on 'Field #', sort by it
            if 'Field #' in combined.columns:
                combined = combined.filter(pl.col('Field #').is_not_null())
                combined = combined.unique(subset=['Field #'], keep='first')
                combined = combined.sort('Field #')

                # Gap check
                vals_list = [int(x) for x in combined['Field #'].to_list() if x is not None]
                if vals_list:
                    min_field = min(vals_list)
                    max_field = max(vals_list)
                    missing_numbers = sorted(set(range(min_field, max_field + 1)) - set(vals_list))
                    if missing_numbers:
                        print(f"Warning: gaps detected in '{pdf_path.name}' Field # range {min_field}-{max_field}: missing {missing_numbers[:20]}{'...' if len(missing_numbers)>20 else ''}")

            out_csv = pdf_path.with_name(f"{pdf_path.stem}_combined.csv")
            out_parquet = pdf_path.with_name(f"{pdf_path.stem}_combined.parquet")

            if not self.options.overwrite_clean_dicts and out_csv.exists() and out_parquet.exists():
                continue

            if 'csv' in formats:
                combined.write_csv(str(out_csv))
            if 'parquet' in formats:
                combined.write_parquet(str(out_parquet))

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
            print(f'Dictionary root does not exist: {root}')
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
            print(f'Processing dictionary tables for year: {y}')
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

        data_path = Path(data_path)
        dictionary_path = Path(dictionary_path)
        if not data_path.exists():
            raise FileNotFoundError(f'Fixed width dataset not found: {data_path}')
        if not dictionary_path.exists():
            raise FileNotFoundError(f'Dictionary file not found: {dictionary_path}')

        fmt = dictionary_format or dictionary_path.suffix.lower().lstrip('.')
        if fmt not in {'csv', 'parquet'}:
            raise ValueError(
                'dictionary_format must be one of {"csv", "parquet"} or inferred '
                f'from path; received: {dictionary_format or dictionary_path.suffix}'
            )

        if fmt == 'csv':
            dict_df = pl.read_csv(dictionary_path)
        else:
            dict_df = pl.read_parquet(dictionary_path)

        if column_name_column not in dict_df.columns:
            raise ValueError(
                f"Dictionary is missing required column '{column_name_column}'. Available columns: {dict_df.columns}"
            )
        if width_column not in dict_df.columns:
            raise ValueError(
                f"Dictionary is missing required column '{width_column}'. Available columns: {dict_df.columns}"
            )

        cleaned = dict_df.select([
            pl.col(column_name_column).cast(pl.Utf8, strict=False).alias('column_name'),
            pl.col(width_column).cast(pl.Int64, strict=False).alias('column_width'),
        ]).drop_nulls('column_width')

        widths: Sequence[int] = [int(w) for w in cleaned['column_width'].to_list() if w is not None]
        if not widths:
            raise ValueError('No usable field widths were found in the dictionary.')
        raw_names: Sequence[str] = [str(name).strip() for name in cleaned['column_name'].to_list()]

        # Ensure column names are unique while preserving order.
        seen: dict[str, int] = {}
        column_names: list[str] = []
        for name in raw_names:
            base = name or 'unnamed'
            count = seen.get(base, 0)
            if count:
                column_names.append(f'{base}_{count + 1}')
            else:
                column_names.append(base)
            seen[base] = count + 1

        # Build explicit column specs that skip a fixed inter-field separator.
        # This handles files where fields are fixed width and separated by
        # exactly `separator_width` characters (commonly a single space), while
        # still parsing correctly when some fields are blank (multiple spaces).
        if separator_width < 0:
            raise ValueError('separator_width must be >= 0')

        colspecs: list[tuple[int, int]] = []
        cursor: int = 0
        for w in widths:
            start = cursor
            end = start + int(w)
            colspecs.append((start, end))
            cursor = end + separator_width

        pd_frame = pd.read_fwf(
            data_path,
            colspecs=colspecs,
            names=column_names,
            header=None,
            encoding=encoding,
        )
        return pl.from_pandas(pd_frame, include_index=False)

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
            print(f'Raw FHFA root does not exist: {raw_root}')
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
                    print(f'Failed resolving dictionary for {data_path.name}: {e}')
                    continue

                if dict_path is None:
                    print(f'No dictionary found for {data_path.name}; skipping.')
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
                    print(f'Failed loading fixed-width for {data_path.name}: {e}')
                    continue

                try:
                    ensure_parent_dir(out_path)
                    df.write_parquet(str(out_path))
                    written.append((data_path, out_path))
                except Exception as e:
                    print(f'Failed writing Parquet for {data_path.name}: {e}')
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
                year_match = self._infer_year_from_name(p.name) or self._infer_year_from_name(p.as_posix())
                enterprise_val = 'fnma' if 'fnma_' in p.name.lower() else ('fhlmc' if 'fhlmc_' in p.name.lower() else None)
                annotated = lf.with_columns([
                    pl.lit(year_match).alias('Year'),
                    pl.lit(enterprise_val).alias('Enterprise'),
                ])
                lazy_frames.append(annotated)
            except Exception as e:
                print(f'Failed to scan {p}: {e}')

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


# -----------------------------
# Conforming Loan Limit Loader
# -----------------------------

class ConformingLoanLimitLoader:
    """Standalone loader for Conforming Loan Limits (CLL) files.

    These are published as yearly Excel workbooks named like
    ``FullCountyLoanLimitList<YEAR>_HERA*.xls*``.
    """

    def __init__(self, paths: Optional[PathsConfig] = None, options: Optional[ImportOptions] = None) -> None:
        self.paths = paths or PathsConfig.from_env()
        self.options = options or ImportOptions()

    def import_conforming_limits(self, data_folder: Path, save_folder: Path, min_year: int = 2011, max_year: int = 2024) -> None:
        """Combine annual CLL Excel sheets into a single gzip-compressed CSV.

        Parameters
        ----------
        data_folder: pathlib.Path
            Directory containing the yearly CLL Excel files.
        save_folder: pathlib.Path
            Output directory for the combined CSV.
        min_year, max_year: int
            Inclusive range of years to search and include if found.
        """

        df_parts: list[pd.DataFrame] = []
        for year in range(min_year, max_year + 1):
            pattern = f'{data_folder}/FullCountyLoanLimitList{year}_HERA*.xls*'
            matches = glob.glob(pattern)
            if not matches:
                continue
            filename = matches[0]
            df_year = pd.read_excel(filename, skiprows=[0], engine=self.options.excel_engine)
            df_year.columns = [str(x).replace('\n', ' ') for x in df_year.columns]
            df_year['Year'] = year
            df_parts.append(df_year)

        if not df_parts:
            return

        df = pd.concat(df_parts)
        ensure_parent_dir(Path(f'{save_folder}/conforming_loan_limits_{min_year}-{max_year}.csv.gz'))
        df.to_csv(
            f'{save_folder}/conforming_loan_limits_{min_year}-{max_year}.csv.gz',
            compression='gzip',
            index=False,
        )

# -----------------------------
# FHLB Loader
# -----------------------------

class FHLBDataLoader:
    """Loader encapsulating FHLB-related ETL steps."""

    def __init__(self, paths: Optional[PathsConfig] = None, options: Optional[ImportOptions] = None) -> None:
        self.paths = paths or PathsConfig.from_env()
        self.options = options or ImportOptions()

    # Download FHLB CSV artifacts
    def download(self, base_url: str, fhlb_csv_dir: Path, include_substrings: Optional[Iterable[str]] = ('pudb',),
                 pause_seconds: Optional[int] = None) -> None:
        """Download FHLB CSV files from the same FHFA page.

        FHLB assets are generally CSVs; apply optional substring filters to avoid unrelated links.
        """
        ps = pause_seconds if pause_seconds is not None else self.options.pause_length_seconds
        _download_from_page(base_url, fhlb_csv_dir, allowed_extensions=['.csv'], included_substrings=include_substrings, pause_seconds=ps)

    # Combine FHLB Member Banks
    def combine_members(self, data_folder: Path, save_folder: Path, min_year: int = 2009, max_year: int = 2023) -> None:
        files: list[str] = []
        for year in range(min_year, max_year + 1):
            files += glob.glob(f'{data_folder}/FHLB_Members*{year}*.xls*')

        colmap = {
            'FHFBID': 'FHFB ID',
            'FHFA ID': 'FHFB ID',
            'MEM NAME': 'MEMBER NAME',
            'MEM TYPE': 'MEMBER TYPE',
            'CHAR TYPE': 'CHARTER TYPE',
            'APPR DATE': 'APPROVAL DATE',
            'CERT': 'FDIC CERTIFICATE NUMBER',
            'FED ID': 'FEDERAL RESERVE ID',
            'OTS ID': 'OTS DOCKET NUMBER',
            'NCUA ID': 'CREDIT UNION CHARTER NUMBER',
        }

        frames: list[pd.DataFrame] = []
        for file in files:
            print('Reading FHLB Members from File:', file)
            # Q1 2020 has bad first line
            if file.replace('\\', '/') == f'{data_folder}/FHLB_Members_Q12020_Release.xlsx':
                df_a = pd.read_excel(file, skiprows=[0], engine=self.options.excel_engine)
            else:
                df_a = pd.read_excel(file, engine=self.options.excel_engine)

            df_a.columns = [str(x).upper().replace('_', ' ').strip() for x in df_a.columns]
            df_a.rename(columns=colmap, inplace=True, errors='ignore')

            filename = file.split('/')[-1]
            df_a['SOURCE FILE'] = filename

            qy = filename.split('Q')[1]
            quarter = int(qy[0])
            year = int(qy[1:5])
            df_a['YEAR'] = year
            df_a['QUARTER'] = quarter

            if (year > 2014) or (year == 2014 and quarter > 2):
                df_a['APPROVAL DATE'] = pd.to_datetime(df_a['APPROVAL DATE'], format='%m/%d/%y')
                df_a['APPROVAL DATE'] = [x - relativedelta(years=100) if x > datetime.datetime.today() else x for x in df_a['APPROVAL DATE']]
                mem_date_series = df_a.get('MEM DATE')
                if mem_date_series is not None:
                    df_a['MEM DATE'] = pd.to_datetime(mem_date_series, format='%m/%d/%y', errors='coerce')
                df_a['MEM DATE'] = [x - relativedelta(years=100) if (pd.notna(x) and x > datetime.datetime.today()) else x for x in df_a['MEM DATE']]

            frames.append(df_a)

        if not frames:
            return

        df = pd.concat(frames)
        df = drop_unnamed_columns(df)
        out_file = f'{save_folder}/fhlb_members_combined_{min_year}-{max_year}.csv.gz'
        ensure_parent_dir(Path(out_file))
        df.to_csv(out_file, compression='gzip', index=False, sep='|')

    # Import FHLB Loan Purchases
    def import_acquisitions_data(self, data_folder: Path, save_folder: Path, dictionary_folder: Path, min_year: int = 2009, max_year: int = 2022) -> None:
        # Map fields safely as str->str
        fields_df = pd.read_csv(f'{dictionary_folder}/fhlb_acquisitions_fields.csv', header=None)
        pairs: Iterable[tuple[str, str]] = [(str(a), str(b)) for a, b in fields_df.iloc[:, 0:2].itertuples(index=False, name=None)]
        field_names = dict(pairs)

        for year in range(min_year, max_year + 1):
            yr_str = str(year)[2:]
            filename = f'{data_folder}/{year}_PUDB_EXPORT_1231{yr_str}.csv'
            df = pd.read_csv(filename)

            # Rename Columns
            df = df.rename(columns=field_names, errors='ignore')

            # Drop Unnamed Columns
            df = drop_unnamed_columns(df)

            # Normalize Percentage Values
            norm_cols = [
                'LTV Ratio Percent',
                'Note Rate Percent',
                'Housing Expense Ratio Percent',
                'Total Debt Expense Ratio Percent',
                'PMI Coverage Percent',
            ]
            for col in norm_cols:
                if col in df.columns:
                    df.loc[df['Year'] < 2019, col] = 100 * df.loc[df['Year'] < 2019, col]

            # Fix Early Income Amounts
            if 'Total Yearly Income Amount' in df.columns:
                df.loc[df['Year'] >= 2019, 'Total Yearly Income Amount'] = df.loc[df['Year'] >= 2019, 'Total Yearly Income Amount'] * 12

            # Fix Indicator Variables Before 2019
            if 'Borrower First Time Homebuyer Indicator' in df.columns:
                df.loc[(df['Year'] < 2019) & (df['Borrower First Time Homebuyer Indicator'] == 2), 'Borrower First Time Homebuyer Indicator'] = 0
            if 'Employment Borrower Self Employment Indicator' in df.columns:
                df.loc[(df['Year'] < 2019) & (df['Employment Borrower Self Employment Indicator'] == 2), 'Employment Borrower Self Employment Indicator'] = 0

            # Fix Census Tract (Multiply by 100 and Round to avoid FPEs)
            if 'Census Tract Identifier' in df.columns:
                df['Census Tract Identifier'] = np.round(100 * df['Census Tract Identifier'])

            # Property Type to Numeric
            if 'Property Type' in df.columns:
                df['Property Type'] = [int(str(x).replace('PT', '')) for x in df['Property Type']]

            # Drop Large DTIs
            if 'Total Debt Expense Ratio Percent' in df.columns:
                df.loc[df['Total Debt Expense Ratio Percent'] >= 1000, 'Total Debt Expense Ratio Percent'] = None
            if 'Housing Expense Ratio Percent' in df.columns:
                df.loc[df['Housing Expense Ratio Percent'] >= 1000, 'Housing Expense Ratio Percent'] = None

            # Replace Columns with Numeric Values for Missings
            replace_cols = {
                'Unit 1 - Number of Bedrooms': [98],
                'Unit 2 - Number of Bedrooms': [98],
                'Unit 3 - Number of Bedrooms': [98],
                'Unit 4 - Number of Bedrooms': [98],
                'Index Source Type': [99],
                'Borrower 1 Age at Application Years Count': [99, 999],
                'Borrower 2 Age at Application Years Count': [98, 99, 998, 999],
                'Housing Expense Ratio Percent': [999, 999.99],
                'Total Debt Expense Ratio Percent': [999, 999.99],
                'Core Based Statistical Area Code': [99999],
                'Margin Rate Percent': [9999, 99999],
                'Geographic Names Information System (GNIS) Feature ID': [9999999999],
                'Unit 1 - Reported Rent': [9999999999],
                'Unit 2 - Reported Rent': [9999999999],
                'Unit 3 - Reported Rent': [9999999999],
                'Unit 4 - Reported Rent': [9999999999],
                'Unit 1 - Reported Rent Plus Utilities': [9999999999],
                'Unit 2 - Reported Rent Plus Utilities': [9999999999],
                'Unit 3 - Reported Rent Plus Utilities': [9999999999],
                'Unit 4 - Reported Rent Plus Utilities': [9999999999],
            }
            for col, missing_values in replace_cols.items():
                if col in df.columns:
                    df.loc[df[col].isin(missing_values), col] = None

            # Convert Prepayment Penalty Expiration Dates
            if 'Prepayment Penalty Expiration Date' in df.columns:
                if year < 2019:
                    df.loc[df['Prepayment Penalty Expiration Date'] == '12/31/9999', 'Prepayment Penalty Expiration Date'] = None
                    df['Prepayment Penalty Expiration Date'] = pd.to_datetime(df['Prepayment Penalty Expiration Date'], errors='coerce', format='%m/%d/%Y')
                else:
                    df.loc[df['Prepayment Penalty Expiration Date'] == '9999-12-31', 'Prepayment Penalty Expiration Date'] = None
                    df['Prepayment Penalty Expiration Date'] = pd.to_datetime(df['Prepayment Penalty Expiration Date'], errors='coerce', format='%Y-%m-%d')

            # Drop Redundant Year Column (Same as Loan Acquisition Date)
            if 'Year' in df.columns:
                df = df.drop(columns=['Year'])

            out_file = f'{save_folder}/fhlb_acquisitions_{year}.parquet'
            ensure_parent_dir(Path(out_file))
            df.to_parquet(out_file, index=False)

    # Combine FHLB Loan Purchases
    def combine_acquisitions_data(self, data_folder: Path, save_folder: Path, min_year: int = 2009, max_year: int = 2022) -> None:
        frames: list[pd.DataFrame] = []
        for year in range(min_year, max_year + 1):
            filename = f'{data_folder}/fhlb_acquisitions_{year}.parquet'
            df_a = pd.read_parquet(filename)
            frames.append(df_a)
        if not frames:
            return
        df = pd.concat(frames)

        out_all = f'{save_folder}/fhlb_acquisitions_{min_year}-{max_year}.parquet'
        ensure_parent_dir(Path(out_all))
        df.to_parquet(out_all, index=False)

        if max_year > 2018 and 'Loan Acquisition Date' in df.columns:
            cols = df.loc[df['Loan Acquisition Date'] >= 2019].dropna(axis=1, how='all').columns
            df_2018p = df.loc[df['Loan Acquisition Date'] >= 2018, cols]
            out_2018p = f'{save_folder}/fhlb_acquisitions_2018-{max_year}.parquet'
            df_2018p.to_parquet(out_2018p, index=False)


__all__ = [
    'PathsConfig',
    'ImportOptions',
    'FHFADataLoader',
    'ConformingLoanLimitLoader',
    'FHLBDataLoader',
]

def debug_run_fhfa_extract(
    overwrite: bool = False,
    zip_dir: Optional[Path] = None,
    dictionary_dir: Optional[Path] = None,
) :
    """Convenience function for IDE interactive debugging.

    - Uses env/default paths via ``PathsConfig.from_env()``
    - Commented-out download steps; only runs extraction

    Parameters
    ----------
    overwrite: bool
        If True, re-write existing extracted files.
    zip_dir: Optional[pathlib.Path]
        Directory containing FHFA .zip files; defaults to ``<raw_dir>/fhfa``.
    dictionary_dir: Optional[pathlib.Path]
        Directory to save dictionary PDFs; defaults to ``<project_dir>/dictionary_files``.
    """
    paths = PathsConfig.from_env()
    options = ImportOptions(overwrite=overwrite)
    fhfa = FHFADataLoader(paths, options)

    fhfa_zip_dir = zip_dir if zip_dir is not None else (paths.raw_dir / 'fhfa')
    dict_dir = dictionary_dir if dictionary_dir is not None else (paths.project_dir / 'dictionary_files')

    print(f'Using project directory: {paths.project_dir}')
    print(f'Raw dir: {paths.raw_dir}')
    print(f'FHFA zip dir: {fhfa_zip_dir}')
    print(f'Dictionary dir: {dict_dir}')

    # Download steps are intentionally commented out (already run)
    # base_url = 'https://www.fhfa.gov/data/pudb'
    # fhfa.download(base_url, fhfa_zip_dir, include_substrings=("pudb",), pause_seconds=None)
    # fhfa.download_dictionaries(base_url, dict_dir, pause_seconds=None)

    # Extract zip contents
    print('Extracting FHFA zip contents...')
    # fhfa.extract_zip_contents(zip_dir=fhfa_zip_dir, dictionary_dir=dict_dir, raw_base_dir=paths.raw_dir)
    print('Extraction complete.')

    # Extract dictionary tables
    print('Extracting FHFA dictionary tables...')
    # fhfa.extract_dictionary_tables_all_years(dictionary_root=dict_dir, output_formats=('csv', 'parquet'), table_settings=None)
    print('Extraction complete.')

    # Resolve data and dictionary paths
    print('Resolving data and dictionary paths...')
    map2023 = fhfa.resolve_dictionaries_for_year_folder(year=2023, raw_fhfa_root=fhfa_zip_dir, dictionary_root=dict_dir)
    for key, value in map2023.items():
        print(key, value)
        print('-' * 100)
    print('Resolution complete.')

    # Convert raw files to parquet
    print('Converting raw files to parquet...')
    # fhfa.convert_all_fixed_width_to_parquet(raw_fhfa_root=fhfa_zip_dir, dictionary_root=dict_dir, output_root=paths.clean_dir / 'fhfa', years=None, encoding='latin-1', separator_width=1, prefer_format='parquet')
    print('Conversion complete.')

    # Combine parquet files by type
    print('Combining parquet files by type...')
    fhfa.combine_parquet_by_type(file_type_pattern='sf{year}c_loans', enterprises=('fnma', 'fhlmc'), min_year=2018, max_year=2023)
    print('Combination complete.')


## Main routine
if __name__ == '__main__':
    try:
        df = debug_run_fhfa_extract(overwrite=False)
        print(df)
    except Exception as e:
        print(e)
