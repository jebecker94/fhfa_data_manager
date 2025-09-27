
    # Deprecated FHFA loader utilities (legacy formats)
    def combine_halves_after_2020(self, year: int, fhfa_folder: Path) -> None:
        """[Deprecated] Merge split loan files from legacy CSV releases.

        Modern FHFA distributions ship as single fixed-width text files inside
        zip archives, so this helper is retained only for historical data that
        arrived as paired CSV "halves" per enterprise.
        """

        frames: list[pd.DataFrame] = []
        for enterprise in ['fhlmc', 'fnma']:
            df_l = pd.read_csv(f'{fhfa_folder}/{enterprise}_sf{year}c_loans_file1.csv.gz', compression='gzip', sep='|')
            df_r = pd.read_csv(f'{fhfa_folder}/{enterprise}_sf{year}c_loans_file2.csv.gz', compression='gzip', sep='|')
            frames.append(
                df_l.merge(
                    df_r,
                    left_on=['Enterprise Flag', 'Record Number'],
                    right_on=['Enterprise Flag', 'Record Number'],
                    how='outer',
                )
            )
        if not frames:
            return

        df = pd.concat(frames)
        out_file = f'{fhfa_folder}/gse_sf{year}c_loans.csv.gz'
        ensure_parent_dir(Path(out_file))
        df.to_csv(out_file, compression='gzip', sep='|', index=False)

    def combine_pre2018(self, first_year: int, last_year: int, raw_fhfa_folder: Path, clean_fhfa_folder: Path) -> None:
        """[Deprecated] Combine annual CSV files from legacy 2010-2017 releases.

        Retained for backward compatibility with older FHFA CSV pipelines; the
        refreshed fixed-width text feeds no longer require this merge logic.
        """

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

    def combine_post2018(self, first_year: int, last_year: int, raw_fhfa_folder: Path, clean_fhfa_folder: Path) -> None:
        """[Deprecated] Combine annual CSV files from legacy 2018+ releases.

        With the fixed-width ZIP distribution, a single text file per year is
        sufficient, so this CSV stitching routine is retained only for legacy
        ingestion flows.
        """

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
        df.loc[
            df['Loan-to-Value Ratio (LTV) at Origination, or Combined LTV (CLTV)'] == 999,
            'Loan-to-Value Ratio (LTV) at Origination, or Combined LTV (CLTV)'
        ] = np.nan
        df.loc[df['Debt-to-Income (DTI) Ratio'] == 99, 'Debt-to-Income (DTI) Ratio'] = np.nan
        df.loc[df['Note Amount'] == 999999999, 'Note Amount'] = np.nan
        df.loc[df['Property Value'] == 999999999, 'Property Value'] = np.nan
        df.loc[df['Introductory Rate Period'] == 999, 'Introductory Rate Period'] = np.nan
        df.loc[df['Number of Borrowers'] == 99, 'Number of Borrowers'] = np.nan
        df.loc[df['First-Time Home Buyer'] == 9, 'First-Time Home Buyer'] = np.nan
        for number in range(1, 6):
            df.loc[df[f'Borrower Race or National Origin {number}'] == 9, f'Borrower Race or National Origin {number}'] = np.nan
            df.loc[
                df[f'Co-Borrower Race or National Origin {number}'] == 9,
                f'Co-Borrower Race or National Origin {number}'
            ] = np.nan
        df.loc[df['Borrower Ethnicity'].isin([4, 9]), 'Borrower Ethnicity'] = np.nan
        df.loc[df['Co-Borrower Ethnicity'].isin([4, 9]), 'Co-Borrower Ethnicity'] = np.nan
        df.loc[df['Borrower Gender'].isin([4, 9]), 'Borrower Gender'] = np.nan
        df.loc[df['Co-Borrower Gender'].isin([4, 9]), 'Co-Borrower Gender'] = np.nan
        df.loc[
            df['Age of Borrower (binned per CFPB specification)'] == 9,
            'Age of Borrower (binned per CFPB specification)'
        ] = np.nan
        df.loc[
            df['Age of Co-Borrower (binned per CFPB specification)'] == 9,
            'Age of Co-Borrower (binned per CFPB specification)'
        ] = np.nan
        df.loc[df['HOEPA Status'] == 9, 'HOEPA Status'] = np.nan
        df.loc[df['Borrower Age 62 or older'] == 9, 'Borrower Age 62 or older'] = np.nan
        df.loc[df['Co-Borrower Age 62 or older'] == 9, 'Co-Borrower Age 62 or older'] = np.nan
        df.loc[df['Preapproval'] == 9, 'Preapproval'] = np.nan
        df.loc[df['Application Channel'] == 9, 'Application Channel'] = np.nan
        df.loc[
            df['Manufactured Home - Land Property Interest'] == 9,
            'Manufactured Home - Land Property Interest'
        ] = np.nan
        df.loc[df['Interest Rate at Origination'] == 99, 'Interest Rate at Origination'] = np.nan
        df.loc[
            df['Automated Underwriting System (AUS) Name'].isin([6, 9]),
            'Automated Underwriting System (AUS) Name'
        ] = np.nan
        df.loc[
            df['Credit Score Model - Borrower'].isin([9, 99]),
            'Credit Score Model - Borrower'
        ] = np.nan
        df.loc[
            df['Credit Score Model - Co-Borrower'].isin([9, 99]),
            'Credit Score Model - Co-Borrower'
        ] = np.nan

        # Census tract strings
        df['census_string_2010'] = (
            df['US Postal State Code'] * 10**9 + df['County - 2010 Census'] * 10**6 + df['Census Tract - 2010 Census']
        )
        df['census_string_2020'] = (
            df['US Postal State Code'] * 10**9 + df['County - 2020 Census'] * 10**6 + df['Census Tract - 2020 Census']
        )
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

    # Deprecated Access loader utilities
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
    
    # [Deprecated] Convert Multifamily Files
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
