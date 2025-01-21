import sqlite3
import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from scipy.stats import linregress
import plotly.express as px
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.pipeline import Pipeline
from matplotlib import dates as mdates
from cycler import cycler
import seaborn as sns



def clean_excel_data_p2(e_df):
    """
    Cleans an Excel DataFrame by dropping rows with missing values and formatting a date column.
    
    Parameters:
    e_df (pd.DataFrame): The input DataFrame to clean.

    Returns:
    pd.DataFrame: The cleaned DataFrame.
    """
    # Define DropNA class
    class DropNA(BaseEstimator, TransformerMixin):
        def fit(self, X, y=None):
            return self
        
        def transform(self, X):
            return X.dropna()

    # Define FormatDate class
    class FormatDate(BaseEstimator, TransformerMixin):
        def __init__(self, date_column):
            self.date_column = date_column
        
        def fit(self, X, y=None):
            return self
        
        def transform(self, X):
            X[self.date_column] = pd.to_datetime(
                X[self.date_column], errors='coerce'
            ).dt.strftime('%Y-%m-%d')
            return X.dropna(subset=[self.date_column])
    
    # Create pipeline
    def create_pipeline(date_column):
        return Pipeline(steps=[
            ('drop_na', DropNA()),
            ('format_date', FormatDate(date_column=date_column))
        ])
    
    # Initialize and run pipeline
    pipeline = create_pipeline(date_column='Date')
    df_cleaned = pipeline.fit_transform(e_df)

    # Return the cleaned DataFrame
    return df_cleaned




def drop_database_p2(db_filename1):
   if os.path.exists(db_filename1):
       os.remove(db_filename1)
       print(f"Database file {db_filename1} has been deleted.")
   else:
       print(f"Database file {db_filename1} does not exist.")




def create_database_p2(db_filename1):
   if os.path.exists(db_filename1):
       os.remove(db_filename1)


   conn = sqlite3.connect(db_filename1)
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
       factor_name TEXT NOT NULL UNIQUE
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
   print(f"Database file {db_filename1} has been created.")




def insert_dates_to_table_p2(db_filename, df):
   if 'Date' not in df.columns:
       print("Error: 'Date' column not found in DataFrame.")
       return
   conn = sqlite3.connect(db_filename)
   cursor = conn.cursor()
   insert_date = "INSERT INTO Dates (date) VALUES (?)"
   df['Date'] = df['Date'].astype(str)
   df = df.dropna(subset=['Date'])
   for index, row in df.iterrows():
       cursor.execute(insert_date, (row['Date'],))
   conn.commit()
   conn.close()
   print("Dates have been inserted into the Dates table.")




def insert_factors_to_table_p2(db_filename, df):
   conn = sqlite3.connect(db_filename)
   cursor = conn.cursor()
   insert_factor = "INSERT INTO Factors (factor_name) VALUES (?)"
   for col in df.columns[df.columns != 'Date']:
       try:
           cursor.execute(insert_factor, (col,))
       except sqlite3.IntegrityError:
           print(f"Factor {col} already exists in the Factors table.")
   conn.commit()
   conn.close()
   print("Factors have been inserted into the Factors table.")




def insert_contributions_to_table_p2(db_filename, df):
   if 'Date' not in df.columns:
       print("Error: 'Date' column not found in DataFrame.")
       return
   conn = sqlite3.connect(db_filename)
   cursor = conn.cursor()
   insert_contribution = "INSERT INTO Contributions (date_id, factor_id, contribution_value) VALUES (?, ?, ?)"
   for index, row in df.iterrows():
       date_id_query = "SELECT date_id FROM Dates WHERE date = ?"
       cursor.execute(date_id_query, (row['Date'],))
       date_id_result = cursor.fetchone()
       if date_id_result:
           date_id = date_id_result[0]
       else:
           print(f"Date {row['Date']} not found in Dates table.")
           continue
       for col in df.columns[df.columns != 'Date']:
           factor_id_query = "SELECT factor_id FROM Factors WHERE factor_name = ?"
           cursor.execute(factor_id_query, (col,))
           factor_id_result = cursor.fetchone()
           if factor_id_result:
               factor_id = factor_id_result[0]
           else:
               print(f"Factor {col} not found in Factors table.")
               continue
           contribution_value = row[col]
           cursor.execute(insert_contribution, (date_id, factor_id, contribution_value))
   conn.commit()
   conn.close()
   print("Contributions have been inserted into the Contributions table.")


def get_pm10_data(db_filename1):
   conn = sqlite3.connect(db_filename1)
   cursor = conn.cursor()


   # Query to retrieve PM10 data along with corresponding dates
   query = """
   SELECT D.date, C.contribution_value
   FROM Contributions C
   JOIN Factors F ON C.factor_id = F.factor_id
   JOIN Dates D ON C.date_id = D.date_id
   WHERE F.factor_name = 'PM10' AND C.contribution_value IS NOT NULL
   ORDER BY D.date ASC;
   """
   cursor.execute(query)
   pm10_data = cursor.fetchall()


   # Convert the data to a pandas DataFrame
   dates = [item[0] for item in pm10_data]
   pm10_values = [item[1] for item in pm10_data]


   df_pm10 = pd.DataFrame({
       'date': dates,
       'total_pm10': pm10_values
   })


   # Close the connection
   conn.close()


   return df_pm10








def plot_pm10_trend(df_pm10):
    # 确保 'date' 列为日期格式
    df_pm10['date'] = pd.to_datetime(df_pm10['date'])
    
    # 提取年份和月份并计算月均值
    df_pm10['year'] = df_pm10['date'].dt.year
    df_pm10['month'] = df_pm10['date'].dt.month
    monthly_avg = (
        df_pm10.groupby(['year', 'month'])['total_pm10']
        .mean()
        .reset_index()
        .sort_values(by=['year', 'month'])
    )
    monthly_avg['month_year'] = pd.to_datetime(
        monthly_avg['year'].astype(str) + '-' + monthly_avg['month'].astype(str)
    )

    # 计算线性回归趋势线
    X = np.arange(len(monthly_avg))
    Y = monthly_avg['total_pm10']
    slope, intercept, r_value, p_value, std_err = linregress(X, Y)

    # 绘制图表
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(
        monthly_avg['month_year'], Y,
        marker='o', linestyle='-', color='#66b3ff', label='PM10 Monthly Average'
    )
    ax.plot(
        monthly_avg['month_year'], slope * X + intercept,
        color='red', linestyle='-', label='Trend Line'
    )
    
    # 设置标题和标签
    ax.set_title('Monthly PM10 Contributions Over Time')
    ax.set_xlabel('Year-Month')
    ax.set_ylabel('PM10 Average Contributions')
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))  # 使用 mdates 格式化
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=12))  # 每隔12个月显示标签
    plt.xticks(rotation=45)
    ax.legend()

    # 添加显著性水平注释
    significance_text = f"{slope:.2f} [{slope - std_err:.2f}, {slope + std_err:.2f}] units/year"
    if p_value < 0.01:
        significance_text += " *** (99% confidence)"
    elif p_value < 0.05:
        significance_text += " ** (95% confidence)"
    elif p_value < 0.1:
        significance_text += " * (90% confidence)"
    ax.text(
        0.5, 0.95, significance_text, 
        transform=ax.transAxes, 
        fontsize=12, color='green', 
        verticalalignment='top', 
        horizontalalignment='center'
    )

    plt.tight_layout()
    return fig






def get_all_factors_data(db_filename1):
   conn = sqlite3.connect(db_filename1)
   cursor = conn.cursor()


   # SQL 查询：获取所有因子的相关数据
   query = """
   SELECT D.date, F.factor_name, C.contribution_value
   FROM Contributions C
   JOIN Factors F ON C.factor_id = F.factor_id
   JOIN Dates D ON C.date_id = D.date_id
   ORDER BY D.date ASC;
   """
   cursor.execute(query)
   all_factors_data = cursor.fetchall()


   # 将数据转化为 Pandas DataFrame
   df_all_factors = pd.DataFrame(all_factors_data, columns=["date", "elements", "contribution_value"])


   # 关闭连接
   conn.close()


   return df_all_factors









def elements_options(df_all_factors):
    """
    提取独特的 elements 作为下拉菜单的选项。
    """
    print("DataFrame columns:", df_all_factors.columns)  # 打印列名
    print("DataFrame preview:\n", df_all_factors.head())  # 打印前几行数据

    if 'elements' not in df_all_factors.columns:
        print("Error: 'elements' column not found in DataFrame.")
        return []  # 返回空列表，避免程序中断

    return df_all_factors['elements'].dropna().unique().tolist()




