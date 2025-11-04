"""FHLB (Federal Home Loan Bank) data loader."""

from __future__ import annotations

import datetime
import glob
import logging
from pathlib import Path
from typing import Iterable, Optional

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

from fhfa_data_manager.config import ImportOptions, PathsConfig, ensure_parent_dir
from fhfa_data_manager.download import download_from_page

logger = logging.getLogger(__name__)


def drop_unnamed_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Drop columns with 'unnamed' in their name (case-insensitive)."""
    unnamed_cols = [c for c in df.columns if isinstance(c, str) and 'unnamed' in c.lower()]
    if unnamed_cols:
        return df.drop(columns=unnamed_cols, errors='ignore')
    return df


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
        download_from_page(base_url, fhlb_csv_dir, allowed_extensions=['.csv'], included_substrings=include_substrings, pause_seconds=ps)

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
            logger.info('Reading FHLB Members from File: %s', file)
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

