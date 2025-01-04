from matplotlib.ticker import FuncFormatter
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import sqlite3
import os
from shiny import App


def clean_data(df):
    # Numbers of dataframe columns
    num_columns = df.shape[1]

    # Keep first two columns and the last three columns
    columns_to_keep = list(range(2)) + list(range(num_columns - 3, num_columns))
    df = df.iloc[:, columns_to_keep]
    df.columns = range(df.shape[1])

    # Locate all whole row NA values
    na_indices = np.where(df.isna().all(axis=1))[0]

    # Initialize dfs
    dfs = []
    if len(na_indices) == 0:
        print("No whole row NA value")
    else:
        na_indices = list(na_indices) + [0, df.shape[0]]
        dfs = [df[na_indices[i]:na_indices[i+1]] for i in range(len(na_indices) - 1)]

    # Obtain each dataframe shape
    shapes = [df.shape for df in dfs]

    # Filter DataFrames with 20 to 25 rows
    filtered_dfs = [df for df, shape in zip(dfs, shapes) if 20 <= shape[0] < 25]

    # Drop the first row NA value of each dataframe
    for i, df in enumerate(filtered_dfs):
        if not df.empty and df.iloc[0].isna().all():
            filtered_dfs[i] = df.drop(df.index[0])

    # Rename filtered_dfs to new_dfs
    new_dfs = [df for df in filtered_dfs if 20 <= df.shape[0] < 25]

    for df in new_dfs:
        # Create a dummy column to identify each df based on its label
        df['Source1'] = df.iloc[0, 0]

        # Using 'for' to split the dummy column
        split_result = df['Source1'].str.split('for', expand=True)
        if split_result.shape[1] == 2:
            df[['Measure', 'Source']] = split_result
        else:
            raise ValueError("String format mismatch in column 'Source1'")
        df.drop('Source1', axis=1, inplace=True)

    # Each dataframe drops the first row (useless)
    for i in range(len(new_dfs)):
        if not new_dfs[i].empty:
            new_dfs[i] = new_dfs[i].iloc[2:].reset_index(drop=True)

    # Rename dataframes columns
    new_column_names = ['Species', 'Base Value', 'DISP Min', 'DISP Average', 'DISP Max', 'Measure', 'Source']
    for df in new_dfs:
        if len(df.columns) == len(new_column_names):
            df.columns = new_column_names

    return new_dfs





def process_dfs_p3(new_dfs):
    # Filter DataFrames where 'Measure' contains 'concentrations'
    selected_dfs = [
        df for df in new_dfs 
        if 'Measure' in df.columns and 
           df['Measure'].notna().all() and 
           (df['Measure'].str.strip().str.lower() == 'concentrations').any()
    ]

    # Calculate new columns
    for i, df_sub in enumerate(selected_dfs):
        # Ensure 'Base Value' exists and is numeric
        if 'Base Value' in df_sub.columns and len(df_sub['Base Value']) > 1:
            df_sub['Base Value'] = pd.to_numeric(df_sub['Base Value'], errors='coerce').fillna(0)
            divisor = df_sub['Base Value'].iloc[0] 
            
            # Print each divisor
            print(f"DataFrame {i+1} divisor: {divisor}")
            
            if pd.notna(divisor) and divisor > 1e-10:
                # Perform calculations
                df_sub.loc[1:, 'Concentration'] = (df_sub['Base Value'].iloc[1:] / divisor) / 1000
                df_sub.loc[1:, 'Average'] = (df_sub['DISP Average'].iloc[1:] / divisor) / 1000
                df_sub.loc[1:, 'Error'] = (df_sub['DISP Min'].iloc[1:] / divisor) / 1000
                df_sub.loc[1:, 'Dispersion'] = (df_sub['DISP Max'].iloc[1:] / divisor) / 1000
            else:
                # Handle divisor as 0 or invalid
                for col in ['Concentration', 'Average', 'Error', 'Dispersion']:
                    df_sub[col] = 0
                print(f"Warning: DataFrame {i+1} divisor is 0 or invalid.")

    # Identify Exceedance DataFrames
    Exceedance_dfs = [
        df[df['Measure'].str.strip().str.lower() == 'percent of species sum']
        for df in new_dfs 
        if 'Measure' in df.columns and not df.empty
    ]

    # Create exceedance mapping
    exceedance_map = {}
    for df in Exceedance_dfs:
        df['Source'] = df['Source'].str.strip()  # Remove spaces
        for _, row in df.iterrows():
            key = (row['Source'], row['Species'])
            exceedance_map[key] = row['Base Value']

    # Combine selected_dfs and exceedance_map
    for df_sub in selected_dfs:
        df_sub['Exceedance'] = float('nan')  
        df_sub['Source'] = df_sub['Source'].str.strip()
        for index, row in df_sub.iterrows():
            key = (row['Source'], row['Species'])
            if key in exceedance_map:
                df_sub.at[index, 'Exceedance'] = exceedance_map[key]
    

    return selected_dfs



def pm_10_percentage(selected_dfs):
    # Select each dataframe's first row with required columns
    first_rows = [
        df.iloc[0][['Source', 'Exceedance']]
        for df in selected_dfs
        if 'Exceedance' in df.columns and 'Source' in df.columns and df.shape[0] > 0
    ]

    # Check if there are valid rows to concatenate
    if not first_rows:
        print("No valid rows found in selected_dfs.")
        return pd.DataFrame()  # Return empty DataFrame if no valid rows

    # Combine each first row into a new DataFrame
    pm_10_percentage = pd.concat(first_rows, axis=1).T  # Transpose to form a DataFrame

    # Rename columns
    pm_10_percentage.rename(columns={'Exceedance': 'Percentage'}, inplace=True)

    # Print and check result
    print("pm_10_percentage DataFrame Shape:", pm_10_percentage.shape)
    print(pm_10_percentage)

    return pm_10_percentage





def source_contribution_transformation(selected_dfs):
    
    transformed_dfs = [df.copy() for df in selected_dfs]

    # remove each dataframe first row
    for i, df in enumerate(transformed_dfs):
        if not df.empty:
            transformed_dfs[i] = df.iloc[1:].reset_index(drop=True)

    # remove useless columns
    columns_to_drop = ['Base Value', 'DISP Min', 'DISP Average', 'DISP Max', 'Measure']
    for i, df in enumerate(transformed_dfs):
        transformed_dfs[i] = df.drop(columns=columns_to_drop, inplace=False, errors='ignore')

    # print and check results
    for i, df in enumerate(transformed_dfs):
        print(f"Processed DataFrame {i+1} Shape: {df.shape}")
        print(df.head())
        print('-' * 40)

    return transformed_dfs







db_filename2 = 'pollution_data.sqlite'

# create database
def create_database_p3(db_filename2):
    if os.path.exists(db_filename2):
        os.remove(db_filename2)
        print(f"Database file {db_filename2} has been deleted.")
    else:
        print(f"Database file {db_filename2} does not exist.")
    
    conn = sqlite3.connect(db_filename2)
    cursor = conn.cursor()

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Species (
        SpeciesID INTEGER PRIMARY KEY AUTOINCREMENT,
        Species TEXT NOT NULL UNIQUE
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS PollutionSources (
        SourceID INTEGER PRIMARY KEY AUTOINCREMENT,
        SourceName TEXT NOT NULL UNIQUE
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Measurement (
        MeasurementID INTEGER PRIMARY KEY AUTOINCREMENT,
        SpeciesID INTEGER,
        SourceID INTEGER,
        Concentration REAL,
        Average REAL,
        Error REAL,
        Dispersion REAL,
        Exceedance REAL,
        FOREIGN KEY (SpeciesID) REFERENCES Species (SpeciesID),
        FOREIGN KEY (SourceID) REFERENCES PollutionSources (SourceID)
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Pm10Percentage (
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        SourceID INTEGER,
        Percentage REAL,
        FOREIGN KEY (SourceID) REFERENCES PollutionSources (SourceID)
    )
    ''')
    
    conn.commit()
    conn.close()
    print(f"Database {db_filename2} created successfully.")






def insert_sources_to_database(db_filename2, processed_dfs):
    """
    Inserts source data from a list of processed DataFrames into the SQLite database.
    
    Parameters:
    - db_filename: str, the name of the SQLite database file.
    - processed_dfs: list of DataFrames, each DataFrame contains columns:
      ['Species', 'Source', 'Concentration', 'Average', 'Error', 'Dispersion', 'Exceedance'].
    """
    # Connect to the database
    conn = sqlite3.connect(db_filename2)
    cursor = conn.cursor()

    try:
        for df in processed_dfs:
            for index, row in df.iterrows():
                # insert values to table "PollutionSources"
                cursor.execute('INSERT OR IGNORE INTO PollutionSources (SourceName) VALUES (?)', (row['Source'],))
        
        # commit
        conn.commit()
    except Exception as e:
        print(f"Error inserting source data: {e}")
        # rollback when error happen
        conn.rollback()
    finally:
        # close connection
        conn.close()
        print("Successfuly inserted data to Source table")





def insert_species_to_database(db_filename2, processed_dfs):
    """
    Inserts species data from a list of processed DataFrames into the SQLite database.
    
    Parameters:
    - db_filename2: str, the name of the SQLite database file.
    - processed_dfs: list of DataFrames, each DataFrame contains columns:
      ['Species', 'Source', 'Concentration', 'Average', 'Error', 'Dispersion', 'Exceedance'].
    """
    # Connect to the database
    conn = sqlite3.connect(db_filename2)
    cursor = conn.cursor()

    try:
        for df in processed_dfs:
            for index, row in df.iterrows():
                # Insert to table Species 
                cursor.execute('INSERT OR IGNORE INTO Species (Species) VALUES (?)', (row['Species'],))
        
        # commit
        conn.commit()
    except Exception as e:
        print(f"Error inserting species data: {e}")
        # rollback when error happen
        conn.rollback()
    finally:
        # close connection
        conn.close()
        print("Data has been inserted to species table")




def insert_measurements_to_database(db_filename2, processed_dfs):
    conn = sqlite3.connect(db_filename2)
    cursor = conn.cursor()

    try:
        for df in processed_dfs:
            if isinstance(df, pd.DataFrame):  # Ensure it's a DataFrame
                print("Processing DataFrame:")
                print(df.head())  # Only print the DataFrame if it's valid

                for _, row in df.iterrows():
                    try:
                        # Obtain SpeciesID
                        cursor.execute('SELECT SpeciesID FROM Species WHERE Species = ?', (row['Species'],))
                        species_id = cursor.fetchone()
                        if species_id:
                            species_id = species_id[0]
                        else:
                            print(f"Species '{row['Species']}' not found in Species table.")
                            continue

                        # Obtain SourceID
                        cursor.execute('SELECT SourceID FROM PollutionSources WHERE SourceName = ?', (row['Source'],))
                        source_id = cursor.fetchone()
                        if source_id:
                            source_id = source_id[0]
                        else:
                            print(f"Source '{row['Source']}' not found in PollutionSources table.")
                            continue

                        # Insert into Measurement
                        cursor.execute('''
                        INSERT INTO Measurement (
                            SpeciesID, SourceID, Concentration, Average, Error, Dispersion, Exceedance
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            species_id,
                            source_id,
                            row['Concentration'],
                            row['Average'],
                            row['Error'],
                            row['Dispersion'],
                            row['Exceedance']
                        ))
                        print(f"Inserted row {row.name}: {row.to_dict()}")
                    except Exception as row_error:
                        print(f"Error inserting row {row.name}: {row.to_dict()} -> {row_error}")
                        continue

        conn.commit()
        print("Data successfully inserted into Measurement table.")
    except Exception as e:
        print(f"Error during database insertion: {e}")
        conn.rollback()
    finally:
        conn.close()
        print("Data has been inserted to Measurement table")






def insert_pm10_percentage_to_db(db_filename, df_pm10_percentage):
    """
    动态处理和插入 PM10 数据，将缺失的来源动态插入 PollutionSources 表。

    Args:
        db_filename (str): 数据库文件名。
        df_pm10_percentage (DataFrame): 包含 Source 和 Percentage 的 pandas 数据帧。
    """

    # 标准化数据
    df_pm10_percentage['Source'] = df_pm10_percentage['Source']

    connection = sqlite3.connect(db_filename)
    cursor = connection.cursor()

    try:
        # 获取现有的 PollutionSources 数据
        cursor.execute("SELECT SourceID, SourceName FROM PollutionSources")
        sources = cursor.fetchall()
        cleaned_sources = {df_pm10_percentage['Source'](row[1]): row[0] for row in sources}
        print(f"Existing sources in database: {cleaned_sources}")

        # 处理每个 Source
        for _, row in df_pm10_percentage.iterrows():
            source = row['Source']
            percentage = row['Percentage']
            print(f"Processing Source: {source}, Percentage: {percentage}")

            # 检查 Source 是否已存在
            if source not in cleaned_sources:
                # 动态插入新的 Source
                cursor.execute(
                    "INSERT INTO PollutionSources (SourceName) VALUES (?)",
                    (source,)
                )
                connection.commit()
                # 获取新插入的 SourceID
                cursor.execute("SELECT SourceID FROM PollutionSources WHERE SourceName = ?", (source,))
                new_source_id = cursor.fetchone()[0]
                cleaned_sources[source] = new_source_id
                print(f"Inserted new source '{source}' with SourceID {new_source_id}.")
            else:
                new_source_id = cleaned_sources[source]
                print(f"Source '{source}' already exists with SourceID {new_source_id}.")

            # 插入 PM10 数据
            cursor.execute(
                '''
                INSERT INTO Pm10Percentage (SourceID, Percentage)
                VALUES (?, ?)
                ''',
                (new_source_id, percentage)
            )
            print(f"Inserted into Pm10Percentage: SourceID={new_source_id}, Percentage={percentage}")

        connection.commit()
        print("All data inserted successfully into Pm10Percentage.")

    except sqlite3.Error as e:
        print(f"Database error: {e}")
        connection.rollback()
    except Exception as e:
        print(f"Unexpected error: {e}")
        connection.rollback()
    finally:
        connection.close()


def fetch_and_format_data(db_filename2):
    """
    Fetch and format data from the database safely.
    """
    try:
        # Step 1: Connect to the database
        conn = sqlite3.connect(db_filename2)
        cursor = conn.cursor()
        
        # Step 2: Execute the query to fetch data
        query = '''
        SELECT 
            Species.Species, 
            PollutionSources.SourceName, 
            Measurement.Concentration, 
            Measurement.Average, 
            Measurement.Error, 
            Measurement.Dispersion, 
            Measurement.Exceedance
        FROM Measurement
        JOIN Species ON Measurement.SpeciesID = Species.SpeciesID
        JOIN PollutionSources ON Measurement.SourceID = PollutionSources.SourceID
        '''
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        # Step 3: Handle the case where no rows are returned
        if not rows:
            print("No data retrieved from the database.")
            return pd.DataFrame(columns=[
                'Species', 'SourceName', 'Concentration', 'Average', 'Error', 'Dispersion', 'Exceedance'
            ])
        
        # Step 4: Create a DataFrame from the query results
        df = pd.DataFrame(rows, columns=[
            'Species', 'SourceName', 'Concentration', 'Average', 'Error', 'Dispersion', 'Exceedance'
        ])
        
        # Step 5: Pivot the DataFrame
        pivot_df = df.pivot_table(
            index='Species', 
            columns='SourceName', 
            values=['Concentration', 'Average', 'Error', 'Dispersion', 'Exceedance']
        )
        
        # Step 6: Flatten MultiIndex columns
        pivot_df.columns = [f"{value}_{col}" for value, col in pivot_df.columns]
        pivot_df.reset_index(inplace=True)
        
        # Debug print for formatted data
        print("Formatted DataFrame preview:\n", pivot_df.head())

        return pivot_df

    except sqlite3.Error as e:
        # Handle SQLite-specific errors
        print(f"SQLite error: {e}")
        return pd.DataFrame({"Error": [f"SQLite error: {e}"]})

    except Exception as e:
        # Handle any other exceptions
        print(f"Unexpected error: {e}")
        return pd.DataFrame({"Error": [str(e)]})

    finally:
        # Step 7: Ensure the database connection is closed
        if conn:
            conn.close()
            print("Database connection closed.")





            


