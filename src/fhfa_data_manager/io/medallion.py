# -*- coding: utf-8 -*-
"""Medallion architecture helpers for FHFA datasets.

This module introduces a light-weight medallion pipeline focused on three
layers:

- raw: ZIP archives or CSV extracts downloaded from external sources
- bronze: minimally processed artifacts (extracted CSVs, normalized file names)
- silver: processed tabular outputs built with Polars transformations

The helpers are intentionally generic so that they can be reused across
multiple FHFA/FHLB ingestion workflows without forcing a specific schema or
set of transformations. Callers can supply custom Polars operations when
building the silver layer while still reusing the staging logic.
"""

from __future__ import annotations

import logging
import shutil
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Literal, Optional, Sequence
from urllib.parse import urlparse

import polars as pl
import requests

from fhfa_data_manager.config import PathsConfig

logger = logging.getLogger(__name__)

LayerName = Literal["raw", "bronze", "silver"]


def _ensure_directory(path: Path) -> None:
    """Ensure that the parent directory of ``path`` exists."""
    path.mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True)
class MedallionPaths:
    """Container describing medallion layer directories for a dataset."""

    dataset_name: str
    base_dir: Path
    raw_dir: Path
    bronze_dir: Path
    silver_dir: Path

    @classmethod
    def create(cls, dataset_name: str, base_dir: Path) -> "MedallionPaths":
        """Construct layer directories for ``dataset_name`` under ``base_dir``."""
        dataset_root = base_dir / dataset_name
        raw_dir = dataset_root / "raw"
        bronze_dir = dataset_root / "bronze"
        silver_dir = dataset_root / "silver"
        for directory in (raw_dir, bronze_dir, silver_dir):
            _ensure_directory(directory)
        return cls(
            dataset_name=dataset_name,
            base_dir=dataset_root,
            raw_dir=raw_dir,
            bronze_dir=bronze_dir,
            silver_dir=silver_dir,
        )


class MedallionPipeline:
    """High-level helper implementing a raw → bronze → silver workflow."""

    def __init__(self, dataset_name: str, *, paths: Optional[PathsConfig] = None) -> None:
        self.paths_config = paths or PathsConfig.from_env()
        self.paths = MedallionPaths.create(dataset_name, self.paths_config.data_dir)

    # ------------------------------------------------------------------
    # Raw layer helpers
    # ------------------------------------------------------------------
    def download_raw_artifact(
        self,
        url: str,
        *,
        file_name: Optional[str] = None,
        overwrite: bool = False,
        chunk_size: int = 1 << 20,
    ) -> Path:
        """Download an external artifact into the raw layer.

        Parameters
        ----------
        url:
            Remote resource to download (typically a ZIP or CSV).
        file_name:
            Optional override for the output file name. If omitted the name is
            inferred from the URL.
        overwrite:
            When ``True`` replace any existing artifact with the same name.
        chunk_size:
            Streaming chunk size used while writing the file.
        """
        parsed = urlparse(url)
        resolved_name = file_name or Path(parsed.path).name
        if not resolved_name:
            raise ValueError("Unable to determine output file name for raw download")

        target = self.paths.raw_dir / resolved_name
        if target.exists() and not overwrite:
            logger.info("Raw artifact already exists at %s; skipping download", target)
            return target

        logger.info("Downloading raw artifact from %s", url)
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()

        _ensure_directory(target.parent)
        with target.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if not chunk:
                    continue
                handle.write(chunk)
        logger.info("Saved raw artifact to %s", target)
        return target

    def save_raw_artifact(self, file_name: str, data: bytes, *, overwrite: bool = False) -> Path:
        """Persist an in-memory artifact to the raw layer."""
        target = self.paths.raw_dir / file_name
        if target.exists() and not overwrite:
            raise FileExistsError(f"Raw artifact already exists: {target}")
        _ensure_directory(target.parent)
        target.write_bytes(data)
        logger.info("Saved raw artifact to %s", target)
        return target

    # ------------------------------------------------------------------
    # Bronze layer helpers
    # ------------------------------------------------------------------
    def promote_to_bronze(
        self,
        raw_artifact: Path,
        *,
        allowed_extensions: Optional[Iterable[str]] = (".csv",),
        overwrite: bool = True,
    ) -> list[Path]:
        """Extract or copy a raw artifact into the bronze layer.

        Raw ZIP files are extracted and flattened into the bronze directory while
        retaining only files that match ``allowed_extensions``. CSV inputs are
        copied directly. The returned list contains the promoted bronze files.
        """
        raw_artifact = raw_artifact.resolve()
        if not raw_artifact.exists():
            raise FileNotFoundError(raw_artifact)

        promoted: list[Path] = []
        allowed = tuple(ext.lower() for ext in allowed_extensions) if allowed_extensions else None

        if raw_artifact.suffix.lower() == ".zip":
            with zipfile.ZipFile(raw_artifact) as archive:
                for member in archive.infolist():
                    if member.is_dir():
                        continue
                    member_path = Path(member.filename)
                    if allowed and member_path.suffix.lower() not in allowed:
                        continue
                    target = self.paths.bronze_dir / member_path.name
                    if target.exists() and not overwrite:
                        logger.debug("Skipping existing bronze file %s", target)
                        promoted.append(target)
                        continue
                    _ensure_directory(target.parent)
                    with archive.open(member) as source, target.open("wb") as destination:
                        shutil.copyfileobj(source, destination)
                    promoted.append(target)
                    logger.info("Promoted %s to bronze layer", target)
        else:
            if allowed and raw_artifact.suffix.lower() not in allowed:
                logger.debug("Raw artifact %s skipped due to extension filter", raw_artifact)
                return []
            target = self.paths.bronze_dir / raw_artifact.name
            if target.exists() and not overwrite:
                logger.debug("Skipping existing bronze file %s", target)
            else:
                _ensure_directory(target.parent)
                shutil.copy2(raw_artifact, target)
                logger.info("Copied raw artifact %s to bronze layer", raw_artifact)
            promoted.append(target)

        return promoted

    def list_layer(self, layer: LayerName) -> list[Path]:
        """Return sorted paths for the requested medallion ``layer``."""
        directory = {
            "raw": self.paths.raw_dir,
            "bronze": self.paths.bronze_dir,
            "silver": self.paths.silver_dir,
        }[layer]
        return sorted(path for path in directory.glob("**/*") if path.is_file())

    # ------------------------------------------------------------------
    # Silver layer helpers
    # ------------------------------------------------------------------
    def build_silver_dataset(
        self,
        *,
        bronze_files: Optional[Sequence[Path]] = None,
        transformations: Optional[Callable[[pl.LazyFrame], pl.LazyFrame]] = None,
        schema: Optional[dict[str, pl.datatypes.PolarsDataType]] = None,
        sink: Literal["parquet", "csv"] = "parquet",
        output_name: Optional[str] = None,
        collect_kwargs: Optional[dict[str, object]] = None,
    ) -> Path:
        """Combine bronze files into a processed silver dataset.

        Parameters
        ----------
        bronze_files:
            Explicit list of bronze files to read. Defaults to all CSV/Parquet
            files in the bronze directory.
        transformations:
            Optional callable that receives a :class:`polars.LazyFrame` and
            returns a transformed LazyFrame. Use this to apply business logic
            before materialising the dataset.
        schema:
            Optional explicit schema passed to :func:`polars.scan_csv` when
            reading CSV sources.
        sink:
            Output format for the silver dataset. Supports ``parquet`` or ``csv``.
        output_name:
            Optional override for the output file name. When omitted a default
            of ``"{dataset_name}_silver.{extension}"`` is used.
        collect_kwargs:
            Additional keyword arguments forwarded to :meth:`LazyFrame.collect`.
        """
        candidates = bronze_files or self.list_layer("bronze")
        if not candidates:
            raise ValueError("No bronze files supplied for silver dataset build")

        lazy_frames: list[pl.LazyFrame] = []
        for file_path in candidates:
            suffix = file_path.suffix.lower()
            if suffix == ".csv":
                lazy = pl.scan_csv(file_path, schema=schema)
            elif suffix in {".parquet", ".pq"}:
                lazy = pl.scan_parquet(file_path)
            else:
                logger.debug("Skipping unsupported bronze file %s", file_path)
                continue
            lazy_frames.append(lazy)

        if not lazy_frames:
            raise ValueError("None of the supplied bronze files could be read")

        combined = pl.concat(lazy_frames)
        if transformations is not None:
            combined = transformations(combined)

        result = combined.collect(**(collect_kwargs or {}))
        extension = "parquet" if sink == "parquet" else "csv"
        resolved_name = output_name or f"{self.paths.dataset_name}_silver.{extension}"
        target = self.paths.silver_dir / resolved_name
        _ensure_directory(target.parent)

        if sink == "parquet":
            result.write_parquet(target)
        elif sink == "csv":
            result.write_csv(target)
        else:
            raise ValueError(f"Unsupported silver sink: {sink}")

        logger.info("Built silver dataset at %s", target)
        return target


__all__ = [
    "LayerName",
    "MedallionPaths",
    "MedallionPipeline",
]

