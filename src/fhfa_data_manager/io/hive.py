"""Utilities for exporting Parquet data into a Hive-partitioned dataset."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, Sequence
import shutil

import polars as pl
import pyarrow.dataset as ds


def parquet_to_hive_dataset(
    sources: Iterable[str | Path],
    destination: str | Path,
    *,
    partition_columns: Sequence[str] = ("year", "enterprise_flag"),
    overwrite: bool = False,
) -> Path:
    """Write one or more Parquet files into a Hive-partitioned dataset.

    Parameters
    ----------
    sources:
        Iterable of file paths or directories containing Parquet files. Directory
        inputs are expanded with a ``**/*.parquet`` glob.
    destination:
        Directory where the Hive dataset should be written.
    partition_columns:
        Column names to use for Hive partitioning. Defaults to ``("year",
        "enterprise_flag")``.
    overwrite:
        When ``True`` and the destination directory already exists it will be
        removed before writing new output. Otherwise a :class:`FileExistsError`
        is raised to prevent accidental data loss.

    Returns
    -------
    pathlib.Path
        The resolved destination directory containing the Hive dataset.
    """
    dest_path = Path(destination).expanduser().resolve()
    if dest_path.exists():
        if not overwrite:
            raise FileExistsError(
                f"Destination directory '{dest_path}' already exists. "
                "Set overwrite=True to replace it."
            )
        shutil.rmtree(dest_path)
    dest_path.mkdir(parents=True, exist_ok=True)

    lazy_frames: list[pl.LazyFrame] = []
    for src in sources:
        src_path = Path(src).expanduser().resolve()
        if src_path.is_dir():
            pattern = str(src_path / "**" / "*.parquet")
            lazy_frames.append(pl.scan_parquet(pattern, recursive=True))
        else:
            lazy_frames.append(pl.scan_parquet(str(src_path)))

    if not lazy_frames:
        raise ValueError("At least one Parquet source must be provided.")

    dataset = pl.concat(lazy_frames)
    table = dataset.collect(streaming=True).to_arrow()

    ds.write_dataset(
        data=table,
        base_dir=str(dest_path),
        format="parquet",
        partitioning=ds.partitioning(field_names=list(partition_columns), flavor="hive"),
        existing_data_behavior="overwrite_or_ignore",
    )

    return dest_path

