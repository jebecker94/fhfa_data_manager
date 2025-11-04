"""Dictionary extraction and resolution utilities for FHFA data files."""

from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import Iterable, Literal, Optional

import fitz  # PyMuPDF
import polars as pl

logger = logging.getLogger(__name__)


def infer_year_from_name(name: str) -> Optional[int]:
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


def resolve_dictionary_for_data_file(
    data_file: Path,
    paths_project_dir: Path,
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
    year = infer_year_from_name(path.name)
    if year is None:
        return None

    dict_root = dictionary_root if dictionary_root is not None else (paths_project_dir / 'dictionary_files')
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


def extract_dictionary_tables_for_year(
    year: int,
    paths_project_dir: Path,
    options_overwrite_clean_dicts: bool,
    *,
    dictionary_root: Optional[Path] = None,
    output_formats: Iterable[Literal['csv', 'parquet']] = ('csv', 'parquet'),
    table_settings: Optional[dict] = None,
) -> None:
    """Extract and combine tables per PDF in ``dictionary_files/<year>`` and save one file per PDF.

    Parameters
    ----------
    year: int
        Year subfolder under the dictionaries root.
    paths_project_dir: Path
        Project directory for default dictionary root.
    options_overwrite_clean_dicts: bool
        Whether to overwrite existing clean dictionary files.
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
    - Honors ``options_overwrite_clean_dicts``. If both outputs already exist and overwrite=False, skips.
    """
    dict_root = dictionary_root if dictionary_root is not None else (paths_project_dir / 'dictionary_files')
    year_dir = Path(dict_root) / str(year)
    if not year_dir.exists() or not year_dir.is_dir():
        logger.warning('Dictionary directory does not exist: %s', year_dir)
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
                        logger.warning('find_tables failed on %s page %s: %s', pdf_path, page_index, e)
                        continue

                    tables_list = getattr(tables_result, 'tables', None) if tables_result is not None else None
                    if not tables_list:
                        continue

                    for table in tables_list:
                        try:
                            pdf_df = table.to_pandas()
                        except Exception as e:
                            logger.warning('to_pandas failed on %s p%s: %s', pdf_path, page_index, e)
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
            logger.error('Failed processing %s: %s', pdf_path, e)
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
                    preview = missing_numbers[:20]
                    ellipsis = '...' if len(missing_numbers) > 20 else ''
                    logger.warning(
                        "Gaps detected in '%s' Field # range %s-%s: missing %s%s",
                        pdf_path.name,
                        min_field,
                        max_field,
                        preview,
                        ellipsis,
                    )

        out_csv = pdf_path.with_name(f"{pdf_path.stem}_combined.csv")
        out_parquet = pdf_path.with_name(f"{pdf_path.stem}_combined.parquet")

        if not options_overwrite_clean_dicts and out_csv.exists() and out_parquet.exists():
            continue

        if 'csv' in formats:
            combined.write_csv(str(out_csv))
        if 'parquet' in formats:
            combined.write_parquet(str(out_parquet))

