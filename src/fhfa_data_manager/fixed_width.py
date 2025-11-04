"""Fixed-width file loading utilities for FHFA data files."""

from __future__ import annotations

from pathlib import Path
from typing import Literal, Optional, Sequence

import pandas as pd
import polars as pl


def load_fixed_width_dataset(
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
        :func:`extract_dictionary_tables_for_year`.
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

