import sqlite3
import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from scipy.stats import linregress


def clean_excel_data_p2(e_df):
   """
   Reads an Excel file into a DataFrame, drops rows with any missing values,
   and prints the sum of missing values before and after dropping.


   Parameters:
   file_path (str): The path to the Excel file.
   sheet_name (str): The name of the sheet to read from the Excel file.
   """


   # Calculate the sum of missing values for each column
   missing_values = e_df.isnull().sum()
   print("Missing values before dropping:")
   print(missing_values)


   # Drop rows with any missing values
   df_cleaned = e_df.dropna()


   # Calculate the sum of missing values again to confirm they are gone
   missing_values_after = e_df.isnull().sum()
   print("\nMissing values after dropping:")
   print(missing_values_after)


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


def plot_pm10_trend(df_pm10, output_file):
   # 确保 'date' 列是日期格式
   df_pm10['date'] = pd.to_datetime(df_pm10['date'])


   # 提取年份和月份
   df_pm10['year'] = df_pm10['date'].dt.year
   df_pm10['month'] = df_pm10['date'].dt.month


   # 分组并计算每月平均 PM10 值
   monthly_avg = df_pm10.groupby(['year', 'month'])['total_pm10'].mean().reset_index()
   monthly_avg['month_year'] = (
   monthly_avg['year'].astype(str) + '-' + monthly_avg['month'].astype(str).str.zfill(2)
)


   # 执行线性回归计算趋势线
   X = np.arange(len(monthly_avg))
   Y = monthly_avg['total_pm10']
   slope, intercept, r_value, p_value, std_err = linregress(X, Y)


   # 绘制图表
   plt.figure(figsize=(12, 6))
   plt.plot(monthly_avg['month_year'], monthly_avg['total_pm10'], marker='o', linestyle='-', color='#66b3ff', markerfacecolor='none', markeredgewidth=2)
   plt.plot(monthly_avg['month_year'], slope * X + intercept, color='red', linestyle='-', label='Trend Line')


   plt.title('Monthly PM10 Contributions Over Time')
   plt.xlabel('Year-Month')
   plt.ylabel('PM10 Average Contributions')
   plt.xticks(ticks=range(0, len(monthly_avg), 12), labels=[str(int(year)) for year in monthly_avg['year'][::12]], rotation=45)
   plt.legend()
   plt.tight_layout()


   # 保存图像
   plt.savefig(output_file)
   plt.close()






def get_all_factors_data(db_filename1):
   conn = sqlite3.connect(db_filename1)
   cursor = conn.cursor()


   # SQL 查询：获取所有因子的相关数据
   query = """
   SELECT D.date, F.factor_name, C.contribution_value
   FROM Contributions C
   JOIN Factors F ON C.factor_id = F.factor_id
   JOIN Dates D ON C.date_id = D.date_id
   WHERE F.factor_name != 'PM10'
   ORDER BY D.date ASC;
   """
   cursor.execute(query)
   all_factors_data = cursor.fetchall()


   # 将数据转化为 Pandas DataFrame
   df_all_factors = pd.DataFrame(all_factors_data, columns=["date", "elements", "contribution_value"])


   # 关闭连接
   conn.close()


   return df_all_factors





import pandas as pd
import matplotlib.pyplot as plt
import tempfile
import os

def plot_all_factors_trend(df_all_factors, output_file):
    """
    Plots the yearly trend of contribution values for each element in the provided dataframe
    and saves the plot to a temporary file.

    Parameters:
    - dataframe (pd.DataFrame): A pandas DataFrame containing the data with columns 'date', 'elements', 'contribution_value', and 'year'.

    Returns:
    - temp_file_path (str): The path to the temporary file where the plot is saved.
    """
    # 将日期字符串转换为datetime对象
    df_all_factors['date'] = pd.to_datetime(df_all_factors['date'])

    # 提取年份
    df_all_factors['year'] = df_all_factors['date'].dt.year

    # 计算每个因素每年的平均贡献值
    yearly_avg = df_all_factors.groupby(['year', 'elements'])['contribution_value'].mean().reset_index()

    # 创建一个临时文件
    temp_file = tempfile.NamedTemporaryFile(suffix='.png', delete=False)
    temp_file_path = temp_file.name

    # 绘制趋势图
    plt.figure(figsize=(12, 8))

    # 为每个因素绘制趋势线
    for element in yearly_avg['elements'].unique():
        element_data = yearly_avg[yearly_avg['elements'] == element]
        plt.plot(element_data['year'], element_data['contribution_value'], marker='o', linestyle='-', label=element)

    # 设置图表标题和轴标签
    plt.title('Yearly Average Contribution of Elements Over Time')
    plt.xlabel('Year')
    plt.ylabel('Average Contribution Value')

    # 添加图例
    plt.legend(title='Element', loc='upper right', ncol=3, labelspacing=0.1, frameon=False)

    # 调整布局并保存图表到临时文件
    plt.tight_layout()
    plt.savefig(temp_file_path, format='png')
    plt.close()

    # 关闭临时文件
    temp_file.close()

    return temp_file_path

