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
from typing import Optional, Iterable, Literal
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


def _download_from_page(base_url: str,
                        download_dir: Path,
                        allowed_extensions: Iterable[str],
                        included_substrings: Optional[Iterable[str]] = None,
                        pause_seconds: int = 5) -> None:
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
        if file_path.exists():
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

    # Download FHFA artifacts (zips and optional PDFs)
    def download(self, base_url: str, fhfa_zip_dir: Path, include_substrings: Optional[Iterable[str]] = ('pudb',),
                 pause_seconds: Optional[int] = None) -> None:
        """Download FHFA-provided zip files (and optionally dictionaries as PDFs).

        The FHFA page uses mixed link labels; filter by extension and optional substrings.
        """
        ps = pause_seconds if pause_seconds is not None else self.options.pause_length_seconds
        # FHFA single-family datasets (zips)
        _download_from_page(base_url, fhfa_zip_dir, allowed_extensions=['.zip'], included_substrings=include_substrings, pause_seconds=ps)

    def download_dictionaries(self, base_url: str, dictionary_dir: Path, pause_seconds: Optional[int] = None) -> None:
        ps = pause_seconds if pause_seconds is not None else self.options.pause_length_seconds
        _download_from_page(base_url, dictionary_dir, allowed_extensions=['.pdf'], included_substrings=None, pause_seconds=ps)

    def extract_zip_contents(self, zip_dir: Path, dictionary_dir: Path, raw_base_dir: Optional[Path] = None) -> None:
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
                            if out_path.exists() and not self.options.overwrite:
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

    # Import Conforming Loan Limits
    def import_conforming_limits(self, data_folder: Path, save_folder: Path, min_year: int = 2011, max_year: int = 2024) -> None:
        df_parts: list[pd.DataFrame] = []
        for year in range(min_year, max_year + 1):
            pattern = f'{data_folder}/FullCountyLoanLimitList{year}_HERA*.xls*'
            matches = glob.glob(pattern)
            if not matches:
                continue
            filename = matches[0]
            df_year = pd.read_excel(filename, skiprows=[0])
            df_year.columns = [str(x).replace('\n', ' ') for x in df_year.columns]
            df_year['Year'] = year
            df_parts.append(df_year)

        if not df_parts:
            return

        df = pd.concat(df_parts)
        ensure_parent_dir(Path(f'{save_folder}/conforming_loan_limits_{min_year}-{max_year}.csv.gz'))
        df.to_csv(f'{save_folder}/conforming_loan_limits_{min_year}-{max_year}.csv.gz',
                  compression='gzip', index=False)

    # Convert Files from Microsoft Access Format to CSV
    def convert_access_files(self, data_folder: Path, save_folder: Path, file_string: str = 'SFCensus') -> None:
        folders = glob.glob(f'{data_folder}/*{file_string}*.zip')
        for folder in folders:
            with zipfile.ZipFile(folder) as z:
                access_files = [x for x in z.namelist() if '.accdb' in x]
                for file in access_files:
                    savename = f'{save_folder}/{file.replace(".accdb", ".csv.gz")}'
                    if Path(savename).exists() and not self.options.overwrite:
                        continue

                    print('Extracting File:', file)
                    try:
                        z.extract(file, path=str(data_folder))
                    except Exception:
                        print('Could not unzip file:', file, 'with Python ZipFile. Using 7z instead.')
                        unzip_string = 'C:/Program Files/7-Zip/7z.exe'
                        p = subprocess.Popen([unzip_string, 'e', f'{folder}', f'-o{data_folder}', f'{file}', '-y'])
                        p.wait()

                    newfilename = f'{data_folder}/{file}'
                    conn_str = (r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
                                r'DBQ=' + newfilename)
                    conn = pyodbc.connect(conn_str)

                    cursor = conn.cursor()
                    table_name = None
                    for i in cursor.tables(tableType='TABLE'):
                        table_name = i.table_name
                        print('Table Name:', table_name)
                        break

                    if not table_name:
                        conn.close()
                        try:
                            os.remove(newfilename)
                        except Exception:
                            pass
                        continue

                    df = pd.read_sql(f'select * from "{table_name}"', conn)
                    ensure_parent_dir(Path(savename))
                    df.to_csv(savename, index=False, sep='|', compression='gzip')
                    conn.close()
                    os.remove(newfilename)

    # Convert Multifamily Files
    def convert_multifamily_files(self, data_folder: Path, save_folder: Path) -> None:
        folders = glob.glob(f'{data_folder}/*MFCensus*.zip')
        # The original code sliced folders[10:], preserving that behavior
        folders = folders[10:]

        col2018 = ['Enterprise Flag', 'Record Number', 'US Postal State Code', 'Metropolitan Statistical Area',
                   'County', 'Census Tract', 'Census Tract Percent Minority', 'Census Tract Median Income', 'Local Area Median Income',
                   'Tract Income Ratio', 'Area Median Family Income', 'Acquisition Unpaid Principal Balance',
                   'Purpose of Loan', 'Type of Seller Institution', 'Federal Guarantee', 'Lien Status',
                   'Loan to Value', 'Date of Mortgage Note', 'Term of Mortgage at Origination', 'Number of Units',
                   'Interest Rate at Origination', 'Note Amount', 'Property Value', 'Prepayment Penalty Term',
                   'Balloon Payment', 'Interest Only', 'Negative Amortization', 'Other Nonamortizing Features',
                   'Multifamily Affordable Units Percent', 'Construction Method', 'Rural Census Tract',
                   'Lower Mississippi Delta County', 'Middle Appalachia County', 'Persistent Poverty County',
                   'Area of Concentrated Poverty', 'High Opportunity Area', 'Qualified Opportunity Zone']

        df_parts: list[pd.DataFrame] = []
        for folder in folders:
            with zipfile.ZipFile(folder) as z:
                loan_files = [x for x in z.namelist() if 'loans.txt' in x]
                for file in loan_files:
                    print('Extracting File:', file)
                    try:
                        z.extract(file, path=str(data_folder))
                    except Exception:
                        print('Could not unzip file:', file, 'with Python ZipFile. Using 7z instead.')
                        unzip_string = 'C:/Program Files/7-Zip/7z.exe'
                        p = subprocess.Popen([unzip_string, 'e', f'{folder}', f'-o{data_folder}', f'{file}', '-y'])
                        p.wait()

                    newfilename = f'{data_folder}/{file}'
                    df_a = pd.read_fwf(newfilename, sep='\t', header=None)
                    df_a.columns = col2018
                    df_a['Year'] = int(file.split('_mf')[1].split('c_')[0])
                    df_parts.append(df_a)

        if not df_parts:
            return

        df = pd.concat(df_parts)
        savename = f'{save_folder}/combined_multifamily.csv'
        ensure_parent_dir(Path(savename))
        df.to_csv(savename, index=False, sep='|')

    # Combine Halves of FHLMC and FNMA Files (after 2020)
    def combine_halves_after_2020(self, year: int, fhfa_folder: Path) -> None:
        frames: list[pd.DataFrame] = []
        for enterprise in ['fhlmc', 'fnma']:
            df_l = pd.read_csv(f'{fhfa_folder}/{enterprise}_sf{year}c_loans_file1.csv.gz', compression='gzip', sep='|')
            df_r = pd.read_csv(f'{fhfa_folder}/{enterprise}_sf{year}c_loans_file2.csv.gz', compression='gzip', sep='|')
            frames.append(df_l.merge(df_r, left_on=['Enterprise Flag', 'Record Number'], right_on=['Enterprise Flag', 'Record Number'], how='outer'))
        df = pd.concat(frames)
        out_file = f'{fhfa_folder}/gse_sf{year}c_loans.csv.gz'
        ensure_parent_dir(Path(out_file))
        df.to_csv(out_file, compression='gzip', sep='|', index=False)

    # Combine Multiple Years (2010 - 2017)
    def combine_pre2018(self, first_year: int, last_year: int, raw_fhfa_folder: Path, clean_fhfa_folder: Path) -> None:
        frames: list[pd.DataFrame] = []
        for year in range(first_year, last_year + 1):
            df_year = pd.read_csv(f'{raw_fhfa_folder}/gse_sf{year}c_loans.csv.gz', compression='gzip', sep='|')
            df_year['year'] = year
            frames.append(df_year)

        if not frames:
            return

        df = pd.concat(frames)
        df['Local Area Median Income'] = df['2012 Local Area Median Income']
        df['Area Median Family Income'] = df['2012 Area Median Family Income']
        drop_cols: list[str] = ['2012 Local Area Median Income', '2012 Area Median Family Income']
        for year in range(2013, 2017 + 1):
            df['Local Area Median Income'].fillna(df.get(f'{year} Local Area Median Income'), inplace=True)
            df['Area Median Family Income'].fillna(df.get(f'{year} Area Median Family Income'), inplace=True)
            drop_cols += [f'{year} Local Area Median Income', f'{year} Area Median Family Income']
        df.drop(columns=drop_cols, inplace=True, errors='ignore')

        out_file = f'{clean_fhfa_folder}/gse_sfc_loans_{first_year}-{last_year}.csv.gz'
        ensure_parent_dir(Path(out_file))
        df.to_csv(out_file, compression='gzip', sep='|', index=False)

    # Combine Multiple Years (2018 - 2021+)
    def combine_post2018(self, first_year: int, last_year: int, raw_fhfa_folder: Path, clean_fhfa_folder: Path) -> None:
        frames: list[pd.DataFrame] = []
        parse_options = pacsv.ParseOptions(delimiter='|')
        for year in range(first_year, last_year + 1):
            file = f'{raw_fhfa_folder}/gse_sf{year}c_loans.csv.gz'
            df_year = pacsv.read_csv(file, parse_options=parse_options).to_pandas(date_as_object=False)
            df_year['year'] = year
            frames.append(df_year)

        if not frames:
            return

        df = pd.concat(frames)

        # Replace Yearly Income Variables
        df['Area Median Family Income'] = np.nan
        df['Local Area Median Income'] = np.nan
        for year in range(first_year, last_year + 1):
            left_col = f'{year} Area Median Family Income'
            right_col = f'{year} Local Area Median Income'
            if left_col in df.columns:
                df['Area Median Family Income'] = df['Area Median Family Income'].fillna(df[left_col])
                df.drop(columns=[left_col], inplace=True, errors='ignore')
            if right_col in df.columns:
                df['Local Area Median Income'] = df['Local Area Median Income'].fillna(df[right_col])
                df.drop(columns=[right_col], inplace=True, errors='ignore')

        # Replace Missing Observations (use inplace loc assignments as in original)
        df.loc[df['Borrower(s) Annual Income'] == 999999998, 'Borrower(s) Annual Income'] = np.nan
        df.loc[df['Discount Points'] == 999999, 'Discount Points'] = np.nan
        df.loc[df['Loan-to-Value Ratio (LTV) at Origination, or Combined LTV (CLTV)'] == 999, 'Loan-to-Value Ratio (LTV) at Origination, or Combined LTV (CLTV)'] = np.nan
        df.loc[df['Debt-to-Income (DTI) Ratio'] == 99, 'Debt-to-Income (DTI) Ratio'] = np.nan
        df.loc[df['Note Amount'] == 999999999, 'Note Amount'] = np.nan
        df.loc[df['Property Value'] == 999999999, 'Property Value'] = np.nan
        df.loc[df['Introductory Rate Period'] == 999, 'Introductory Rate Period'] = np.nan
        df.loc[df['Number of Borrowers'] == 99, 'Number of Borrowers'] = np.nan
        df.loc[df['First-Time Home Buyer'] == 9, 'First-Time Home Buyer'] = np.nan
        for number in range(1, 6):
            df.loc[df[f'Borrower Race or National Origin {number}'] == 9, f'Borrower Race or National Origin {number}'] = np.nan
            df.loc[df[f'Co-Borrower Race or National Origin {number}'] == 9, f'Co-Borrower Race or National Origin {number}'] = np.nan
        df.loc[df['Borrower Ethnicity'].isin([4, 9]), 'Borrower Ethnicity'] = np.nan
        df.loc[df['Co-Borrower Ethnicity'].isin([4, 9]), 'Co-Borrower Ethnicity'] = np.nan
        df.loc[df['Borrower Gender'].isin([4, 9]), 'Borrower Gender'] = np.nan
        df.loc[df['Co-Borrower Gender'].isin([4, 9]), 'Co-Borrower Gender'] = np.nan
        df.loc[df['Age of Borrower (binned per CFPB specification)'] == 9, 'Age of Borrower (binned per CFPB specification)'] = np.nan
        df.loc[df['Age of Co-Borrower (binned per CFPB specification)'] == 9, 'Age of Co-Borrower (binned per CFPB specification)'] = np.nan
        df.loc[df['HOEPA Status'] == 9, 'HOEPA Status'] = np.nan
        df.loc[df['Borrower Age 62 or older'] == 9, 'Borrower Age 62 or older'] = np.nan
        df.loc[df['Co-Borrower Age 62 or older'] == 9, 'Co-Borrower Age 62 or older'] = np.nan
        df.loc[df['Preapproval'] == 9, 'Preapproval'] = np.nan
        df.loc[df['Application Channel'] == 9, 'Application Channel'] = np.nan
        df.loc[df['Manufactured Home - Land Property Interest'] == 9, 'Manufactured Home - Land Property Interest'] = np.nan
        df.loc[df['Interest Rate at Origination'] == 99, 'Interest Rate at Origination'] = np.nan
        df.loc[df['Automated Underwriting System (AUS) Name'].isin([6, 9]), 'Automated Underwriting System (AUS) Name'] = np.nan
        df.loc[df['Credit Score Model - Borrower'].isin([9, 99]), 'Credit Score Model - Borrower'] = np.nan
        df.loc[df['Credit Score Model - Co-Borrower'].isin([9, 99]), 'Credit Score Model - Co-Borrower'] = np.nan

        # Census tract strings
        df['census_string_2010'] = df['US Postal State Code'] * 10**9 + df['County - 2010 Census'] * 10**6 + df['Census Tract - 2010 Census']
        df['census_string_2020'] = df['US Postal State Code'] * 10**9 + df['County - 2020 Census'] * 10**6 + df['Census Tract - 2020 Census']
        df['census_string'] = None
        df['census_string'] = df['census_string'].where(df['year'] >= 2022, df['census_string_2010'], axis=0)
        df['census_string'] = df['census_string'].where(df['year'] < 2022, df['census_string_2020'], axis=0)
        df['census_string'] = df['census_string'].astype('Int64').astype('str')
        df['census_string'] = [x.zfill(11) for x in df['census_string']]
        df.drop(columns=['census_string_2010', 'census_string_2020'], inplace=True, errors='ignore')

        # Save CSV and Parquet
        dt = pa.Table.from_pandas(df, preserve_index=False)

        write_options = pacsv.WriteOptions(delimiter='|')
        save_csv = f'{clean_fhfa_folder}/gse_sfc_loans_clean_{first_year}-{last_year}.csv.gz'
        ensure_parent_dir(Path(save_csv))
        with pa.CompressedOutputStream(save_csv, 'gzip') as out:
            pacsv.write_csv(dt, out, write_options=write_options)

        save_parquet = f'{clean_fhfa_folder}/gse_sfc_loans_clean_{first_year}-{last_year}.parquet'
        pq.write_table(dt, save_parquet)

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

                            # Standardize and expand single or range values (e.g., "19-23")
                            work = df.rename({field_col: 'Field #'})
                            # keep original as text
                            work = work.with_columns([
                                pl.col('Field #').cast(pl.Utf8, strict=False).alias('field_text')
                            ])
                            # parse start and optional end
                            work = work.with_columns([
                                pl.col('field_text').str.extract(r'(\d+)', 1).cast(pl.Int64, strict=False).alias('field_start'),
                                pl.col('field_text').str.extract(r'\d+\s*-\s*(\d+)', 1).cast(pl.Int64, strict=False).alias('field_end'),
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
                            ]).drop(['field_text', 'field_start', 'field_end', 'field_end2', 'FieldNums'])
                            # Drop rows with empty Field #
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

            if not self.options.overwrite and out_csv.exists() and out_parquet.exists():
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
    'FHLBDataLoader',
]

def debug_run_fhfa_extract(
    overwrite: bool = False,
    zip_dir: Optional[Path] = None,
    dictionary_dir: Optional[Path] = None,
) -> None:
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
    fhfa.extract_zip_contents(zip_dir=fhfa_zip_dir, dictionary_dir=dict_dir, raw_base_dir=paths.raw_dir)
    print('Extraction complete.')

    # Extract dictionary tables
    print('Extracting FHFA dictionary tables...')
    # fhfa.extract_dictionary_tables_for_year(year=2023, dictionary_root=dict_dir, output_formats=('csv', 'parquet'), table_settings=None)
    fhfa.extract_dictionary_tables_all_years(dictionary_root=dict_dir, output_formats=('csv', 'parquet'), table_settings=None)
    print('Extraction complete.')


## Main routine
if __name__ == '__main__':
    debug_run_fhfa_extract()
