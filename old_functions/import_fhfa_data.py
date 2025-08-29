# -*- coding: utf-8 -*-
"""
Created on Tue Sep 20 15:28:59 2022
@author: Jonathan E. Becker
"""

# Import Packages
import os
import glob
import zipfile
import pyodbc
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import csv
import subprocess
import config

#%% Local Functions
# Import Conforming Loan Limits
def import_conforming_limits(data_folder, save_folder, min_year = 2011, max_year = 2024) :
    """
    Import conforming data on yearly conforming loan limits.

    Parameters
    ----------
    data_folder : str
        Folder where yearly raw files are stored.
    save_folder : str
        Folder where combined and cleaned file will be saved.
    min_year : int, optional
        First year of conforming loan limits to include. The default is 2011.
    max_year : int, optional
        Last year of conforming loan limits to include. The default is 2024.

    Returns
    -------
    None.

    """

    # Combine Yearly
    df = []
    for year in range(min_year, max_year + 1) :

        # File Name
        filename = glob.glob(f'{data_folder}/FullCountyLoanLimitList{year}_HERA*.xls*')[0]

        # Read and Rename Columns
        df_a = pd.read_excel(filename, skiprows=[0],)
        df_a.columns = [x.replace('\n',' ') for x in df_a.columns]
        df_a['Year'] = year
        
        # Append
        df.append(df_a)

    # Concatenate and Save
    df = pd.concat(df)
    df.to_csv(f'{save_folder}/conforming_loan_limits_{min_year}-{max_year}.csv.gz',
              compression = 'gzip',
              index = False,
              )

# Convert Files from Microsoft Access Format to CSV
def convert_fhfa_access_files(data_folder, save_folder, file_string = 'SFCensus', overwrite = False) :
    """
    Converts files from microsoft access .accdb format into .csv.gz for easy use in Stata
    
    Parameters
    ----------
    data_folder : string
        Folder where zipped .accdb files are located.
    save_folder : string
        Folder where gzipped csvs are to be saved.

    Returns
    -------
    None.

    """
    
    # Get Zip Folders
    folders = glob.glob(f'{data_folder}/*{file_string}*.zip')
    
    for folder in folders :

        with zipfile.ZipFile(folder) as z :

            # Only Worry about Access Files
            access_files = [x for x in z.namelist() if ".accdb" in x]

            for file in access_files :
                
                # New File Name
                savename = f'{save_folder}/{file.replace(".accdb", ".csv.gz")}'
                
                # Only Proceed if File Doesn't Exist or Overwrite Mode in On
                if not os.path.exists(savename) or overwrite :

                    # Extract and Create Temporary File
                    print('Extracting File:', file)
                    try :
                        z.extract(file, path = data_folder)
                    except :
                        print('Could not unzip file:', file, 'with Pythons Zipfile package. Using 7z instead.')
                        unzip_string = "C:/Program Files/7-Zip/7z.exe"
                        p = subprocess.Popen([unzip_string, "e", f"{folder}", f"-o{data_folder}", f"{file}", "-y"])
                        p.wait()

                    # Connect to File
                    newfilename = f'{data_folder}/{file}'
                    conn_str = (r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
                                r'DBQ='+newfilename)
                    conn = pyodbc.connect(conn_str)

                    # Get Table Name
                    cursor = conn.cursor()
                    for i in cursor.tables(tableType = 'TABLE') :
                        table_name = i.table_name
                        print('Table Name:', table_name)
    
                    # Read in to to DataFrame
                    df = pd.read_sql(f'select * from "{table_name}"', conn)
    
                    # Save
                    df.to_csv(savename,
                              index = False,
                              sep = "|",
                              compression = 'gzip',
                              )

                    # Close Access Database and Remove Temporary File
                    conn.close()
                    os.remove(newfilename)

# Convert Multifamily Files
def convert_fhfa_multifamily_files(data_folder, save_folder, overwrite = False) :
    """
    Unzip and convert FHFA multifamily files.

    Parameters
    ----------
    data_folder : str
        Folder where zip files are stored.
    save_folder : str
        Folder where unzipped and cleaned multifamily files are saved.
    overwrite : boolean, optional
        Whether to overwrite filex with the same target name. The default is False.

    Returns
    -------
    None.

    """
    
    # Get Zip Folders
    folders = glob.glob(f'{data_folder}/*MFCensus*.zip')
    folders = folders[10:]
    
    # Column Names (2018-)
    col2018 = ['Enterprise Flag', 'Record Number', 'US Postal State Code', 'Metropolitan Statistical Area',
               'County', 'Census Tract', 'Census Tract Percent Minority', 'Census Tract Median Income', 'Local Area Median Income',
               'Tract Income Ratio', 'Area Median Family Income', 'Acquisition Unpaid Principal Balance',
               'Purpose of Loan', 'Type of Seller Institution', 'Federal Guarantee', 'Lien Status',
               'Loan to Value', 'Date of Mortgage Note', 'Term of Mortgage at Origination', 'Number of Units',
               'Interest Rate at Origination', 'Note Amount', 'Property Value', 'Prepayment Penalty Term',
               'Balloon Payment', 'Interest Only', 'Negative Amortization', 'Other Nonamortizing Features',
               'Multifamily Affordable Units Percent', 'Construction Method', 'Rural Census Tract', 
               'Lower Mississippi Delta County', 'Middle Appalachia County', 'Persistent Poverty County',
               'Area of Concentrated Poverty', 'High Opportunity Area', 'Qualified Opportunity Zone',]
    
    df = []
    for folder in folders :

        with zipfile.ZipFile(folder) as z :

            # Only Worry about Loan Files
            loan_files = [x for x in z.namelist() if "loans.txt" in x]

            for file in loan_files :

                # Extract and Create Temporary File
                print('Extracting File:', file)
                try :
                    z.extract(file, path = data_folder,)
                except :
                    print('Could not unzip file:', file, 'with Pythons Zipfile package. Using 7z instead.')
                    unzip_string = "C:/Program Files/7-Zip/7z.exe"
                    p = subprocess.Popen([unzip_string, "e", f"{folder}", f"-o{data_folder}", f"{file}", "-y"])
                    p.wait()
                    
                # Connect to File
                newfilename = f'{data_folder}/{file}'
                df_a = pd.read_fwf(newfilename, sep = '\t', header = None,)
                df_a.columns = col2018
                df_a['Year'] = int(file.split('_mf')[1].split('c_')[0])
                df.append(df_a)
    
    # Combine and Save
    df = pd.concat(df)
    savename = f'{save_folder}/combined_multifamily.csv'
    df.to_csv(savename, index=False, sep="|")

# Combine Halves of FHLMC and FNMA Files
def combine_fhfa_halves_after_2020(year, fhfa_folder) :
    """
    Combine FNMA and FHLMC files into a single yearly file (after 2020 only).

    Parameters
    ----------
    year : int
        Year of data to combine.
    fhfa_folder : str
        Folder where cleaned file parts are stored and will be saved.

    Returns
    -------
    None.

    """

    df = []
    for enterprise in ['fhlmc','fnma'] :
        df_l = pd.read_csv(f'{fhfa_folder}/{enterprise}_sf{year}c_loans_file1.csv.gz',
                    compression = 'gzip',
                    sep = '|',
                    )
        df_r = pd.read_csv(f'{fhfa_folder}/{enterprise}_sf{year}c_loans_file2.csv.gz',
                    compression = 'gzip',
                    sep = '|',
                    )
        df = df + [df_l.merge(df_r,
                        left_on=['Enterprise Flag','Record Number'],
                        right_on=['Enterprise Flag','Record Number'],
                        how='outer',
                        )]

    # Combine and Save
    df = pd.concat(df,)
    df.to_csv(f'{fhfa_folder}/gse_sf{year}c_loans.csv.gz',
              compression = 'gzip',
              sep = '|',
              index = False,
              )

# Combine Multiple Years (2010 - 2017)
def combine_fhfa_pre2018(first_year, last_year, raw_fhfa_folder, clean_fhfa_folder):
    """
    Combine FHFA yearly data before 2018.

    Parameters
    ----------
    first_year : int
        First year of data to combine.
    last_year : int
        Last year of data to combine.
    raw_fhfa_folder : str
        Folder where yearly FHFA files are stored.
    clean_fhfa_folder : str
        Folder where combined file will be saved.

    Returns
    -------
    None.

    """

    # Combine Yearly Files
    df = []
    for year in range(first_year, last_year+1) :
        df_year = pd.read_csv(f'{raw_fhfa_folder}/gse_sf{year}c_loans.csv.gz',
                              compression = 'gzip',
                              sep = '|',
                              )
        df_year['year'] = year
        df.append(df_year)

    # Combine and Fix Yearly Variables
    df = pd.concat(df)
    df['Local Area Median Income'] = df['2012 Local Area Median Income']
    df['Area Median Family Income'] = df['2012 Area Median Family Income']
    drop_cols = ['2012 Local Area Median Income', '2012 Area Median Family Income']
    for year in range(2013, 2017+1) :
        df['Local Area Median Income'].fillna(df[f'{year} Local Area Median Income'], inplace = True,)
        df['Area Median Family Income'].fillna(df[f'{year} Area Median Family Income'], inplace = True,)
        drop_cols += [f'{year} Local Area Median Income', f'{year} Area Median Family Income']
    df.drop(columns = drop_cols, inplace = True,)

    # Save
    df.to_csv(f'{clean_fhfa_folder}/gse_sfc_loans_{first_year}-{last_year}.csv.gz',
              compression = 'gzip',
              sep = '|',
              index = False,
              )
    
# Combine Multiple Years (2018 - 2021)
def combine_fhfa_post2018(first_year, last_year, raw_fhfa_folder, clean_fhfa_folder):
    """
    Combine FHFA yearly data after 2018.

    Parameters
    ----------
    first_year : int
        First year of data to combine.
    last_year : int
        Last year of data to combine.
    raw_fhfa_folder : str
        Folder where yearly FHFA files are stored.
    clean_fhfa_folder : str
        Folder where combined file will be saved.

    Returns
    -------
    None.

    """

    # Combine Yearly Files
    df = []
    parse_options = csv.ParseOptions(delimiter='|')
    for year in range(first_year, last_year+1) :
        file = f'{raw_fhfa_folder}/gse_sf{year}c_loans.csv.gz'
        df_year = csv.read_csv(file, parse_options=parse_options).to_pandas(date_as_object=False)
        df_year['year'] = year
        df.append(df_year)
        del df_year
    df = pd.concat(df)

    # Replace Yearly Income Variables
    df['Area Median Family Income'] = np.nan
    df['Local Area Median Income'] = np.nan
    for year in range(first_year, last_year+1) :
        df['Area Median Family Income'] = df['Area Median Family Income'].fillna(df[f'{year} Area Median Family Income'])
        df['Local Area Median Income'] = df['Local Area Median Income'].fillna(df[f'{year} Local Area Median Income'])
        df = df.drop(columns = [f'{year} Area Median Family Income'], errors='ignore')
        df = df.drop(columns = [f'{year} Local Area Median Income'], errors='ignore')

    # Replace Missing Observations
    df.loc[df['Borrower(s) Annual Income'] == 999999998, 'Borrower(s) Annual Income'] = np.nan
    df.loc[df['Discount Points'] == 999999, 'Discount Points'] = np.nan
    df.loc[df['Loan-to-Value Ratio (LTV) at Origination, or Combined LTV (CLTV)'] == 999, 'Loan-to-Value Ratio (LTV) at Origination, or Combined LTV (CLTV)'] = np.nan
    df.loc[df['Debt-to-Income (DTI) Ratio'] == 99, 'Debt-to-Income (DTI) Ratio'] = np.nan
    df.loc[df['Note Amount'] == 999999999, 'Note Amount'] = np.nan
    df.loc[df['Property Value'] == 999999999, 'Property Value'] = np.nan
    df.loc[df['Introductory Rate Period'] == 999, 'Introductory Rate Period'] = np.nan
    df.loc[df['Number of Borrowers'] == 99, 'Number of Borrowers'] = np.nan
    df.loc[df['First-Time Home Buyer'] == 9, 'First-Time Home Buyer'] = np.nan
    for number in range(1, 6) :
        df.loc[df[f'Borrower Race or National Origin {number}'] == 9, f'Borrower Race or National Origin {number}'] = np.nan
        df.loc[df[f'Co-Borrower Race or National Origin {number}'] == 9, f'Co-Borrower Race or National Origin {number}'] = np.nan
    df.loc[df['Borrower Ethnicity'].isin([4,9]), 'Borrower Ethnicity'] = np.nan
    df.loc[df['Co-Borrower Ethnicity'].isin([4,9]), 'Co-Borrower Ethnicity'] = np.nan
    df.loc[df['Borrower Gender'].isin([4,9]), 'Borrower Gender'] = np.nan
    df.loc[df['Co-Borrower Gender'].isin([4,9]), 'Co-Borrower Gender'] = np.nan
    df.loc[df['Age of Borrower (binned per CFPB specification)'] == 9, 'Age of Borrower (binned per CFPB specification)'] = np.nan
    df.loc[df['Age of Co-Borrower (binned per CFPB specification)'] == 9, 'Age of Co-Borrower (binned per CFPB specification)'] = np.nan
    df.loc[df['HOEPA Status'] == 9, 'HOEPA Status'] = np.nan
    df.loc[df['Borrower Age 62 or older'] == 9, 'Borrower Age 62 or older'] = np.nan
    df.loc[df['Co-Borrower Age 62 or older'] == 9, 'Co-Borrower Age 62 or older'] = np.nan
    df.loc[df['Preapproval'] == 9, 'Preapproval'] = np.nan
    df.loc[df['Application Channel'] == 9, 'Application Channel'] = np.nan
    df.loc[df['Manufactured Home - Land Property Interest'] == 9, 'Manufactured Home - Land Property Interest'] = np.nan
    df.loc[df['Interest Rate at Origination'] == 99, 'Interest Rate at Origination'] = np.nan
    df.loc[df['Automated Underwriting System (AUS) Name'].isin([6,9]), 'Automated Underwriting System (AUS) Name'] = np.nan
    df.loc[df['Credit Score Model - Borrower'].isin([9,99]), 'Credit Score Model - Borrower'] = np.nan
    df.loc[df['Credit Score Model - Co-Borrower'].isin([9,99]), 'Credit Score Model - Co-Borrower'] = np.nan

    # Census Tract to String
    df['census_string_2010'] = df['US Postal State Code']*10**9 + df['County - 2010 Census']*10**6 + df['Census Tract - 2010 Census']
    df['census_string_2020'] = df['US Postal State Code']*10**9 + df['County - 2020 Census']*10**6 + df['Census Tract - 2020 Census']
    df['census_string'] = None
    df['census_string'] = df['census_string'].where(df['year'] >= 2022, df['census_string_2010'], axis = 0)
    df['census_string'] = df['census_string'].where(df['year'] < 2022, df['census_string_2020'], axis = 0)
    df['census_string'] = df['census_string'].astype('Int64')
    df['census_string'] = df['census_string'].astype('str')
    df['census_string'] = [x.zfill(11) for x in df['census_string']]
    df = df.drop(columns = ['census_string_2010', 'census_string_2020'])
    # df = df.drop(columns = ['US Postal State Code', 'County - 2010 Census', 'Census Tract - 2010 Census'])

    ## Save
    dt = pa.Table.from_pandas(df, preserve_index=False)

    # Save as CSV
    write_options = csv.WriteOptions(delimiter = '|')
    save_file = f'{save_folder}/gse_sfc_loans_clean_{first_year}-{last_year}.csv.gz'
    with pa.CompressedOutputStream(save_file, 'gzip') as out :
        csv.write_csv(dt, out, write_options=write_options)

    # Save as Parquet
    save_file_parquet = f'{save_folder}/gse_sfc_loans_clean_{first_year}-{last_year}.parquet'
    pq.write_table(dt, save_file_parquet)

#%% Main Routine
if __name__ == '__main__' :

    # Setup: Define Base Folder
    DATA_DIR = config.DATA_DIR
    RAW_DIR = config.RAW_DIR
    CLEAN_DIR = config.CLEAN_DIR

    # Display Access Drivers
    driver_names = [i for i in pyodbc.drivers() if i.startswith('Microsoft Access Driver')]

    # Conforming Loan Limits
    data_folder = RAW_DIR / 'conforming_loan_limits'
    save_folder = DATA_DIR
    # import_conforming_limits(data_folder, save_folder, min_year = 2011, max_year = 2024)

    # FHFA
    data_folder = RAW_DIR / 'zip_files'
    save_folder = CLEAN_DIR
    # convert_fhfa_access_files(data_folder, save_folder, file_string = '2023*SFCensus')
    # combine_fhfa_halves_after_2020(2023, save_folder)
    # convert_fhfa_multifamily_files(master_folder, save_folder)
    
    # Combine Data
    data_folder = CLEAN_DIR
    save_folder = DATA_DIR
    # combine_fhfa_pre2018(2010, 2017, data_folder, save_folder)
    # combine_fhfa_post2018(2018, 2023, data_folder, save_folder)
