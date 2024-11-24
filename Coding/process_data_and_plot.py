import pandas as pd
import sqlite3
import os

def process_data_and_plot(dataframe):
    """
    Placeholder for data processing and plotting logic.
    """
    pass  # Implement your logic here

def split_and_adjust_data(dataframe):
    full_missing_rows = dataframe[dataframe.isna().all(axis=1)]
    if full_missing_rows.empty:
        return dataframe, pd.DataFrame()
    last_missing_index = full_missing_rows.index[-1]
    df = dataframe.iloc[last_missing_index + 1:]
    df.columns = dataframe.iloc[2]
    df = df[3:].reset_index(drop=True)
    df.columns = ['Base Run', 'Date'] + df.columns[2:].tolist()
    return df

def replace_negative_with_mean(df):
    for column in df.columns:
        if pd.api.types.is_numeric_dtype(df[column]):
            column_mean = df[column][df[column] >= 0].mean()
            df[column] = df[column].apply(lambda x: column_mean if x < 0 else x)
    return df

def drop_nan_columns(df):
    return df.dropna(axis=1, how='any')

def drop_database(db_filename):
    if os.path.exists(db_filename):
        os.remove(db_filename)
        print(f"Database file {db_filename} has been deleted.")
    else:
        print(f"Database file {db_filename} does not exist.")

def create_database(db_filename):
    if os.path.exists(db_filename):
        os.remove(db_filename)
    
    conn = sqlite3.connect(db_filename)
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Dates (
        date_id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL
    );
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Factors (
        factor_id INTEGER PRIMARY KEY AUTOINCREMENT,
        factor_name TEXT NOT NULL
    );
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Contributions_Value (
        contribution_id INTEGER PRIMARY KEY AUTOINCREMENT,
        date_id INTEGER,
        factor_id INTEGER,
        contribution_value REAL,
        FOREIGN KEY (date_id) REFERENCES Dates(date_id),
        FOREIGN KEY (factor_id) REFERENCES Factors(factor_id)
    );
    """)
    conn.commit()
    conn.close()

def insert_dates_to_table(db_filename, df):
    if 'Date' not in df.columns:
        return False
    conn = sqlite3.connect(db_filename)
    cursor = conn.cursor()
    insert_date = "INSERT INTO Dates (date) VALUES (?)"
    df['Date'] = df['Date'].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else None)
    
    try:
        for _, row in df.iterrows():
            cursor.execute(insert_date, (row['Date'],))
        conn.commit()
    except sqlite3.Error as e:
        print(f"Error: {e}")
        return False
    finally:
        conn.close()
    return True

def insert_factors_to_table(db_filename, df):
    # Connecting to a SQLite Database
    conn = sqlite3.connect(db_filename)
    cursor = conn.cursor()

    for column_name in df.columns[2:]:
        if pd.isnull(column_name) or column_name == '':
            raise ValueError(f"Column name cannot be NULL or empty: {column_name}")

    # Prepare SQL statements to insert data
    insert_factor = """
    INSERT INTO Factors (factor_name) VALUES (?)
    """

    # Traverse the column names of df1 starting from the third column and insert them into the database
    for column_name in df.columns[2:]:  # Starting from the third column, index is 2
        # Perform an insert operation
        cursor.execute(insert_factor, (column_name,))

    # confirm operation
    conn.commit()

    # close connection
    conn.close()


def insert_contributions_to_table(db_filename, df):
    conn = sqlite3.connect(db_filename)
    cursor = conn.cursor()
    try:
        get_date_id = "SELECT date_id FROM Dates WHERE date = ?"
        get_factor_id = "SELECT factor_id FROM Factors WHERE factor_name = ?"
        insert_contribution = "INSERT INTO Contributions_Value (date_id, factor_id, contribution_value) VALUES (?, ?, ?)"
        
        for index, row in df.iterrows():
            cursor.execute(get_date_id, (str(row['Date']),))
            date_id_result = cursor.fetchone()
            if date_id_result:
                date_id = date_id_result[0]
                for column_name in df.columns[2:]:
                    cursor.execute(get_factor_id, (str(column_name),))
                    factor_id_result = cursor.fetchone()
                    if factor_id_result:
                        factor_id = factor_id_result[0]
                        cursor.execute(insert_contribution, (date_id, factor_id, row[column_name]))
                    else:
                        print(f"No matching factor_id found for factor {column_name}")
            else:
                print(f"No matching date_id found for date {row['Date']}")
    except sqlite3.Error as e:
        print(f"Error: {e}")
    finally:
        conn.commit()
        conn.close()
    print("Contributions have been inserted into the Contributions table.")

def fetch_contributions(db_filename):
    conn = sqlite3.connect(db_filename)
    query = """
    SELECT D.date,
           SUM(C.contribution_value) AS total_pm10
    FROM Contributions_Value C
    JOIN Dates D ON C.date_id = D.date_id
    GROUP BY D.date
    ORDER BY D.date;
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df