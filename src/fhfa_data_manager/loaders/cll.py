"""Conforming Loan Limit (CLL) data loader."""

from __future__ import annotations

import glob
import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from fhfa_data_manager.config import ImportOptions, PathsConfig, ensure_parent_dir

logger = logging.getLogger(__name__)


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

