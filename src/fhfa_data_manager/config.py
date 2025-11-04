# -*- coding: utf-8 -*-
"""
Configuration dataclasses and utilities for FHFA data management.

This module provides:
- PathsConfig: typed, centralized path configuration
- ImportOptions: operational flags for data loaders
- Helper utilities for path management
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Optional

logger = logging.getLogger(__name__)


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
    def from_env(
        cls,
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


def ensure_parent_dir(path: Path) -> None:
    """Ensure that the parent directory of ``path`` exists."""
    path.parent.mkdir(parents=True, exist_ok=True)
