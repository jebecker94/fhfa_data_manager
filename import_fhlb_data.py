#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun 18 08:20:27 2023
@author: Jonathan E. Becker
"""

# Import Packages
import datetime
import glob
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
import config

#%% Local Functions
# Combine FHLB Member Banks
def combine_fhlb_members(data_folder, save_folder, min_year=2009, max_year=2023) :
    """
    Combine quarterly FHLB membership data.

    Parameters
    ----------
    data_folder : str
        Folder where membership data is stored.
    save_folder : str
        Folder where cleaned yearly data will be saved.
    min_year : int, optional
        First year to include. The default is 2009.
    max_year : int, optional
        Last year to include. The default is 2023.

    Returns
    -------
    None.

    """

    # Files
    files = []
    for year in range(min_year, max_year+1) :
        files += glob.glob(f'{data_folder}/FHLB_Members*{year}*.xls*')

    # Column Map
    colmap = {'FHFBID': 'FHFB ID',
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

    # Combine Yearly Member Files
    df = []
    for file in files :

        # Display Progress
        print("Reading FHLB Members from File:", file)

        # Read Data (Careful: Q1 2020 has bad first line)
        if file in [f'{data_folder}/FHLB_Members_Q12020_Release.xlsx', f'{data_folder}\\FHLB_Members_Q12020_Release.xlsx'] :
            df_a = pd.read_excel(file, skiprows=[0], engine='calamine')
        else :
            df_a = pd.read_excel(file, engine='calamine')

        # Standardize Column Names
        df_a.columns =[x.upper().replace('_',' ').strip() for x in df_a.columns]
        df_a.rename(columns = colmap, inplace = True, errors = 'ignore')

        # Add Source File Name
        filename = file.split('/')[-1]
        df_a['SOURCE FILE'] = filename

        # Add Year/Quarter
        qy = filename.split('Q')[1]
        quarter = int(qy[0])
        year = int(qy[1:5])
        df_a['YEAR'] = year
        df_a['QUARTER'] = quarter

        # Convert Dates after Q2 2014
        if (year > 2014) | (year == 2014 & quarter > 2) :
            df_a['APPROVAL DATE'] = pd.to_datetime(df_a['APPROVAL DATE'], format = '%m/%d/%y')
            df_a['APPROVAL DATE'] = [x - relativedelta(years = 100) if x > datetime.datetime.today() else x for x in df_a['APPROVAL DATE']]
            df_a['MEM DATE'] = pd.to_datetime(df_a['MEM DATE'], format = '%m/%d/%y', errors = 'coerce')
            df_a['MEM DATE'] = [x - relativedelta(years = 100) if x > datetime.datetime.today() else x for x in df_a['MEM DATE']]

        # Append Quarterly Data
        df.append(df_a)
        del df_a

    # Concatenate
    df = pd.concat(df)

    # Drop Unnamed Columns
    unnamed_cols = [x for x in df.columns if "unnamed" in x.lower()]
    df.drop(columns = unnamed_cols, inplace = True)
    
    # Save
    df.to_csv(f'{save_folder}/fhlb_members_combined_{min_year}-{max_year}.csv.gz',
              compression = 'gzip',
              index = False,
              sep = '|',
              )

# Import FHLB Loan Purchases
def import_fhlb_acquisitions_data(data_folder, save_folder, dictionary_folder, min_year = 2009, max_year = 2022) :
    """
    Imports, cleans, and saves yearly acquisitions data for the FHLB system.

    Parameters
    ----------
    data_folder : str
        Folder containing raw yearly acquisition files.
    save_folder : str
        Folder where cleaned yearly acquisitions data will be saved.
    dictionary_folder : str
        Folder containing field name mappings for standardizing columns.
    min_year : int, optional
        First year of acquisitions data to import. The default is 2009.
    max_year : int, optional
        Last year of acquisitions data to import. The default is 2022.

    Returns
    -------
    None.

    """

    # Rename Old Common Variables
    fields = pd.read_csv(f'{dictionary_folder}/fhlb_acquisitions_fields.csv', header = None)
    field_names = {x[0]:x[1] for i,x in fields.iterrows()}

    # Import and Clean Yearly Files
    for year in range(min_year, max_year+1) :

        # Import File
        yr_str = str(year)[2::]
        filename = f'{data_folder}/{year}_PUDB_EXPORT_1231{yr_str}.csv'
        df = pd.read_csv(filename)

        ## Cleaning
        # Rename Columns
        df = df.rename(columns = field_names, errors='ignore')

        # Drop Unnamed Columns
        unnamed_cols = [x for x in df.columns if "unnamed" in x.lower()]
        df = df.drop(columns = unnamed_cols, errors='ignore')

        # Normalize Percentage Values
        norm_cols = ['LTV Ratio Percent',
                     'Note Rate Percent',
                     'Housing Expense Ratio Percent',
                     'Total Debt Expense Ratio Percent',
                     'PMI Coverage Percent']
        for col in norm_cols :
            df.loc[df['Year'] < 2019, col] = 100*df.loc[df['Year'] < 2019, col]

        # Fix Early Income Amounts
        df.loc[df['Year'] >= 2019, 'Total Yearly Income Amount'] = df.loc[df['Year'] >= 2019, 'Total Yearly Income Amount']*12

        # Fix Indicator Variables Before 2019
        df.loc[(df['Year'] < 2019) & (df['Borrower First Time Homebuyer Indicator'] == 2), 'Borrower First Time Homebuyer Indicator'] = 0
        df.loc[(df['Year'] < 2019) & (df['Employment Borrower Self Employment Indicator'] == 2), 'Employment Borrower Self Employment Indicator'] = 0

        # Fix Census Tract (Multiply by 100 and Round to avoid FPEs)
        df['Census Tract Identifier'] = np.round(100*df['Census Tract Identifier'])

        # Property Type to Numeric
        df['Property Type'] = [int(x.replace("PT","")) for x in df['Property Type']]

        # Drop Large DTIs
        df.loc[df['Total Debt Expense Ratio Percent'] >= 1000, 'Total Debt Expense Ratio Percent'] = None
        df.loc[df['Housing Expense Ratio Percent'] >= 1000, 'Housing Expense Ratio Percent'] = None

        # Replace Columns with Numeric Values for Missings
        replace_cols = {'Unit 1 - Number of Bedrooms': [98],
                        'Unit 2 - Number of Bedrooms': [98],
                        'Unit 3 - Number of Bedrooms': [98],
                        'Unit 4 - Number of Bedrooms': [98],
                        'Index Source Type': [99],
                        'Borrower 1 Age at Application Years Count': [99,999],
                        'Borrower 2 Age at Application Years Count': [98,99,998,999],
                        'Housing Expense Ratio Percent': [999,999.99],
                        'Total Debt Expense Ratio Percent': [999,999.99],
                        'Core Based Statistical Area Code': [99999],
                        'Margin Rate Percent': [9999,99999],
                        'Geographic Names Information System (GNIS) Feature ID': [9999999999],
                        'Unit 1 - Reported Rent': [9999999999],
                        'Unit 2 - Reported Rent': [9999999999],
                        'Unit 3 - Reported Rent': [9999999999],
                        'Unit 4 - Reported Rent': [9999999999],
                        'Unit 1 - Reported Rent Plus Utilities': [9999999999],
                        'Unit 2 - Reported Rent Plus Utilities': [9999999999],
                        'Unit 3 - Reported Rent Plus Utilities': [9999999999],
                        'Unit 4 - Reported Rent Plus Utilities': [9999999999]}
        for col in replace_cols.items() :
            if col[0] in df.columns :
                df.loc[df[col[0]].isin(col[1]), col[0]] = None

        # Convert Prepayment Penalty Expiration Dates
        if year < 2019 :
            df.loc[df['Prepayment Penalty Expiration Date'] == '12/31/9999', 'Prepayment Penalty Expiration Date'] = None
            df['Prepayment Penalty Expiration Date'] = pd.to_datetime(df['Prepayment Penalty Expiration Date'], errors = 'coerce', format = "%m/%d/%Y")
        else :
            df.loc[df['Prepayment Penalty Expiration Date'] == '9999-12-31', 'Prepayment Penalty Expiration Date'] = None
            df['Prepayment Penalty Expiration Date'] = pd.to_datetime(df['Prepayment Penalty Expiration Date'], errors = 'coerce', format = "%Y-%m-%d")

        # Drop Redundant Year Column (Same as Loan Acquisition Date)
        df = df.drop(columns = ['Year'])

        # Save as Parquet
        save_file_name = f'{save_folder}/fhlb_acquisitions_{year}.parquet'
        df.to_parquet(save_file_name, index=False)

# Combine FHLB Loan Purchases
def combine_fhlb_acquisitions_data(data_folder, save_folder, min_year=2009, max_year=2022) :
    """
    Combines FHLB acquisitions data.

    Parameters
    ----------
    data_folder : str
        Folder where cleaned yearly FHLB acquisitions data is stored.
    save_folder : str
        Folder where combined and cleaned acquisitions data will be saved.
    min_year : int, optional
        First year to include. The default is 2009.
    max_year : int, optional
        Last year to include. The default is 2022.

    Returns
    -------
    None.

    """

    # Combine Yearly Files
    df = []
    for year in range(min_year, max_year+1) :
        filename = f'{data_folder}/fhlb_acquisitions_{year}.parquet'
        df_a = pd.read_parquet(filename)
        df.append(df_a)
    df = pd.concat(df)

    # Save Full Data
    df.to_parquet(f'{save_folder}/fhlb_acquisitions_{min_year}-{max_year}.parquet', index=False)

    # Save Data After 2018
    if max_year > 2018 :
        cols = df[df['Loan Acquisition Date'] >= 2019].dropna(axis=1, how='all').columns
        df = df[df['Loan Acquisition Date'] >= 2018][cols]
        df.to_parquet(f'{save_folder}/fhlb_acquisitions_2018-{max_year}.parquet', index=False)

#%% Main Routine
if __name__ == '__main__' :

    # Setup: Define Base Folder
    DATA_DIR = config.DATA_DIR
    RAW_DIR = config.RAW_DIR
    CLEAN_DIR = config.CLEAN_DIR
    PROJECT_DIR = config.PROJECT_DIR

    # Import Raw
    data_folder = RAW_DIR / 'fhlb'
    save_folder = CLEAN_DIR / 'fhlb'
    dictionary_folder = PROJECT_DIR / 'dictionary_files'
    # import_fhlb_acquisitions_data(data_folder, save_folder, dictionary_folder, min_year=2009, max_year=2023)

    # FHLB Acquisitions
    data_folder = CLEAN_DIR / 'fhlb'
    save_folder = DATA_DIR
    # combine_fhlb_acquisitions_data(data_folder, save_folder, min_year=2009, max_year=2023)

    # FHLB Members
    data_folder = RAW_DIR / 'fhlb'
    save_folder = DATA_DIR
    # combine_fhlb_members(data_folder, save_folder, min_year=2009, max_year=2024)
