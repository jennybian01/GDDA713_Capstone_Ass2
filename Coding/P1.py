from matplotlib import pyplot as plt
import pandas as pd
import sqlite3
import os

def process_data_and_plot(dataframe):
    """
    Process data and generate a plot for trend analysis.
    This integrates the complete P1 logic and returns a matplotlib plot.
    """
    # Function definitions based on your original code
    def split_and_adjust_data(df):
        full_missing_rows = df[df.isna().all(axis=1)]
        if full_missing_rows.empty:
            return df, pd.DataFrame()
        last_missing_index = full_missing_rows.index[-1]
        df1 = df.iloc[:last_missing_index]
        df2 = df.iloc[last_missing_index + 1:]
        df1.columns = df1.iloc[2]
        df1 = df1[3:].reset_index(drop=True)
        df2.columns = df2.iloc[2]
        df2 = df2[3:].reset_index(drop=True)
        df1.columns = ['Base Run', 'Date'] + df1.columns[2:].tolist()
        df2.columns = ['Base Run', 'Date'] + df2.columns[2:].tolist()
        new_columns = [col if pd.notna(col) else None for col in df2.columns]
        new_columns = [col for col in new_columns if col is not None]
        if new_columns[2] is None:
            new_columns[2] = new_columns[3]
            new_columns[3] = 'Unknown'
        else:
            new_columns.append('Unknown')
        if len(new_columns) < len(df2.columns):
            new_columns.append('Unknown')
        df2.columns = new_columns
        df2.columns = df1.columns
        return df1, df2

    def replace_negative_with_mean(df):
        for column in df.columns:
            if pd.api.types.is_numeric_dtype(df[column]):
                column_mean = df[column][df[column] >= 0].mean()
                if pd.isna(column_mean):
                    column_mean = 0
                df[column] = df[column].apply(lambda x: column_mean if x < 0 else x)
        return df

    def drop_nan_columns(df):
        return df.dropna(axis=1, how='any')

    def create_database(db_filename):
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
        CREATE TABLE IF NOT EXISTS Contributions (
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
            return
        conn = sqlite3.connect(db_filename)
        cursor = conn.cursor()
        insert_date = "INSERT INTO Dates (date) VALUES (?)"
        df['Date'] = df['Date'].astype(str)
        df = df.dropna(subset=['Date'])
        for _, row in df.iterrows():
            cursor.execute(insert_date, (row['Date'],))
        conn.commit()
        conn.close()

    def fetch_contributions(db_filename):
        conn = sqlite3.connect(db_filename)
        query = """
        SELECT D.date,
               SUM(C.contribution_value) AS total_pm10
        FROM Contributions C
        JOIN Dates D ON C.date_id = D.date_id
        GROUP BY D.date
        ORDER BY D.date;
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df

    def plot_monthly_pm10_trend(df):
        df['date'] = pd.to_datetime(df['date'])
        df['year'] = df['date'].dt.year
        df['month'] = df['date'].dt.month
        monthly_avg = df.groupby(['year', 'month'])['total_pm10'].mean().reset_index()
        monthly_avg['month_year'] = monthly_avg['year'].astype(str) + '-' + monthly_avg['month'].astype(str).str.zfill(2)
        plt.figure(figsize=(12, 6))
        plt.plot(monthly_avg['month_year'], monthly_avg['total_pm10'], marker='o', linestyle='-', color='#66b3ff', markerfacecolor='none', markeredgewidth=2)
        plt.title('Monthly PM10 Contributions Over Time')
        plt.xlabel('Year')
        plt.ylabel('PM10 Average Contributions')
        plt.xticks(ticks=range(0, len(monthly_avg['month_year']), 12), 
                   labels=monthly_avg['month_year'].values[::12], 
                   rotation=45)
        plt.grid(axis='y')
        return plt

    # Main P1 logic
    db_filename = 'trend_analysis_contributions.db'
    df1, df2 = split_and_adjust_data(dataframe)
    df1 = df1.drop(columns=['Base Run'])
    df2 = df2.drop(columns=['Base Run'])
    df2 = replace_negative_with_mean(df2)
    df2 = drop_nan_columns(df2)
    create_database(db_filename)
    insert_dates_to_table(db_filename, df2)
    contributions_df = fetch_contributions(db_filename)
    return plot_monthly_pm10_trend(contributions_df)
