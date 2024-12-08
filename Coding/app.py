from shiny import App, ui, render, req, reactive
from shiny import session
import pandas as pd
import os
import numpy as np
import sqlite3
import tempfile
from p1.process_data_and_plot import (
    process_data_and_plot,
    fetch_contributions,
    create_database,
    insert_dates_to_table,
    split_and_adjust_data,
    replace_negative_with_mean,
    drop_nan_columns,
    insert_factors_to_table,
    insert_contributions_to_table,
    plot_monthly_pm10_trend,
)
from datetime import date
import matplotlib.pyplot as plt
import random
import plotly.express as px
import seaborn as sns
from shinywidgets import render_plotly
from shinywidgets import output_widget, render_widget
import calplot
from p2.process_data_and_plot_p2 import (
    clean_excel_data_p2,
    drop_database_p2,
    create_database_p2,
    insert_dates_to_table_p2,
    insert_factors_to_table_p2,
    insert_contributions_to_table_p2,
    get_pm10_data,
    plot_pm10_trend,
    get_all_factors_data,
    plot_all_factors_trend,
    elements_options,
)


db_filename = "contributions_data.sqlite"
db_filename1 = "elements_data.sqlite"

# Define CSS styles
css = """
#tab {
    width: 100%; 
}
#tab .nav-item {
    background-color: #f9f9f9;
    padding: 20px;
    border-radius: 15px;
    margin-bottom: 20px;
    text-align: center; 
}
#tab .nav-link {
    color: #5081c8;
    font-weight: bold; /* 加粗文字 */
}
#tab .nav-link.active {
    background-color: #e7e7e7;
}
#tab .tab-content {
    background-color: #f9f9f9;
    padding: 10px;
    border-radius: 5px;
}
"""






# Define the UI
app_ui = ui.page_fluid(
    ui.card_header(
        "Auckland Air Quality Monitoring Application",
        style="background-color: #5081c8; color: white; padding: 60px; text-align: center; font-size: 40px;",
    ),
    ui.navset_pill_list(
        ui.nav_panel(
            "Contributions Trend Analysis",
            ui.navset_tab(
                ui.nav_panel(
                    "Upload",
                    "Please upload excel file.",
                    ui.input_file(
                        "file1",
                        "Choose an Excel file to upload, eg. RUN5PM10_base.xlsx",
                        multiple=False,
                    ),
                    ui.output_table(
                        "preview_table1"
                    ),  # Ensure this matches the server function
                ),
                ui.nav_panel(
                    "Data processing",
                    "Cleaning and processing dataset.",
                    ui.h5("Total PM10 group by date"),
                    ui.output_data_frame(
                        "processed_contributions"
                    ),  # Output processed contributions data
                    ui.download_button("downloadData1", "Download Data"),
                ),
                ui.nav_panel(
                    "Trend analysis",
                    "Cleaned data trend analysis",
                    ui.output_plot("monthly_pm10_trend_plot"),
                ),
            ),
        ),
        ui.nav_panel(
            "Element Trend Analysis",
            "Trend analysis of each element",
            ui.navset_tab(
                ui.nav_panel(
                    "Upload",
                    "Please upload excel file.",
                    ui.input_file(
                        "file2",
                        "Choose an Excel file to upload, eg.QueenStreetPM10PMFDataUncert_May2023.xlsx ",
                        multiple=False,
                    ),
                    ui.output_table("preview_table2"),  # Preview for file2
                ),
                ui.nav_panel(
                    "Data processing(PM10)",
                    "Cleaning and processing dataset.",
                    ui.h5("Total PM10 by date"),
                    ui.output_data_frame("processed_pm10_table"),
                    ui.download_button("downloadData2", "Download Data"),
                ),
                ui.nav_panel(
                    "Data processing(All elements)",
                    "Cleaning and processing dataset.",
                    ui.h5("Elements group by date"),
                    ui.output_data_frame("processed_all_factors_table"),
                    ui.download_button("downloadData3", "Download Data"),
                ),
                ui.nav_panel(
                    "Trend Analysis(PM10)",
                    "Cleaned data trend analysis",
                    ui.input_action_button("action_button1", "Visualization"),
                    ui.output_plot("pm10_trend_plot"),
                ),
                ui.nav_panel(
                    "Trend Analysis(All elements)",
                    "Cleaned data trend analysis",
                    ui.input_action_button("action_button2", "Visualization"),
                    ui.output_plot("all_factors_trend_plot"),
                ),
                ui.nav_panel(
                    "Calendar Plot",
                    "Shows the selected element trend from calendar plot",
                    ui.input_selectize(id="element_dropdown", label="Select an Element:", choices=[]),
                    ui.input_action_button("action_botton3", label="Visualization"),
                    ui.output_text_verbatim("selected_option"),
                    ui.output_plot("plot_calendar_new"),
                ),
            ),
        ),
        ui.nav_panel(
            "Source Contribution",
            "Pie Chart & Source Contribution Plot",
            ui.navset_tab(
                ui.nav_panel(
                    "Upload",
                    "Please upload excel file.",
                    ui.input_file(
                        "file3", "Choose an Excel file to upload", multiple=False
                    ),
                    ui.output_table("preview_table3"),  # Preview for file3
                ),
                ui.nav_panel("Data processing", "Cleaning and processing dataset"),
                ui.nav_panel("Pie Chart", "Source Contribution Pie Chart"),
                ui.nav_panel("Source Contribution Plot", "Source Contribution Plot"),
            ),
        ),
        id="tab",
    ),
    ui.tags.style(css),  # Add CSS styles
)


dropdown_options = reactive.Value(elements_options)


def server(input, output, session):
    print("Registered outputs:", dir(output))

    # Reactive variable to store the fetched contributions data
    contributions_data = reactive.Value(pd.DataFrame())

    # Reactive variable to store the fetched pm10_df and all factors
    get_pm10_data_reactive = reactive.Value()
    get_all_factors_data_reactive = reactive.Value()
    uploaded_data = reactive.Value(None)

    df_data = None  # 在全局作用域中初始化

    def initialize_data():
        global df_data
        data = get_all_factors_data_reactive() or {"date": [], "elements": [], "contribution_value": []}
        df_data = pd.DataFrame(data)

    @output
    @render.table
    def preview_table1():
        """
        Render the first 10 rows of the uploaded Excel file.
        """
        file_info = input.file1()
        req(file_info)  # Ensure a file is uploaded
        file_path = file_info[0]["datapath"]
        df = pd.read_excel(file_path, sheet_name="Contributions")
        uploaded_data.set(df)  # Store the uploaded data
        return df.head(10)

    @reactive.Effect
    @reactive.event(input.file1)
    def process_and_store_data():
        """
        Trigger data processing when a file is uploaded, using the 'Contribution' worksheet.
        """
        df = uploaded_data()
        req(df is not None and not df.empty)  # Ensure data is valid and non-empty

        # Delete existing database file
        if os.path.exists(db_filename):
            os.remove(db_filename)

        # Re-create the database
        create_database(db_filename)

        # Data cleaning and processing logic from p1.process_data_and_plot
        df = split_and_adjust_data(df)
        df = replace_negative_with_mean(df)
        df_clean = drop_nan_columns(df)

        # Store cleaned data into SQLite database
        insert_dates_to_table(db_filename, df_clean)
        insert_factors_to_table(db_filename, df_clean)
        insert_contributions_to_table(db_filename, df_clean)


    

    @output
    @render.data_frame
    def processed_contributions():
        """
        Display the first 10 rows of fetched contributions data.
        """
        # Fetch and store processed contributions data
        contributions_data.set(fetch_contributions(db_filename))
        # Return the first 10 rows of the fetched data
        return contributions_data().iloc[:10, :]

    @render.download(
        filename=lambda: f"data-{date.today().isoformat()}-{random.randint(100, 999)}.csv"
    )
    def downloadData():
        df_download1 = contributions_data()
        if not df_download1.empty:
            csv_data = df_download1.to_csv(index=False)
            yield csv_data

    @output
    @render.plot
    def monthly_pm10_trend_plot():
        df = fetch_contributions(db_filename)
        if not df.empty:
            plot_result = plot_monthly_pm10_trend(df)
            if plot_result is not None:
                return plot_result
            else:
                return None
        else:
            return None

    ########################## P2 ######################################
    @output
    @render.table
    def preview_table2():
        """
        Render the first 10 rows of the uploaded Excel file for file2.
        """
        if input.file2() is None:
            return pd.DataFrame({"Message": ["No file uploaded yet."]})
        file_info = input.file2()
        uploaded_file_data = pd.read_excel(
            file_info[0]["datapath"], sheet_name="PMFData"
        )
        return uploaded_file_data.head(10)

    @reactive.Effect
    @reactive.event(input.file2)
    def process_and_store_data_p2():
        """
        Trigger data processing when a file is uploaded, using the 'PMFData' worksheet.
        """

        file_info = input.file2()
        print("File info:", file_info)
        if file_info is not None:
            file_path = file_info[0].get("datapath")
            if file_path:
                print("File path:", file_path)
                e_df = pd.read_excel(file_path, sheet_name="PMFData")
            else:
                print("File path is missing in file_info.")

        # Delete existing database file
        if os.path.exists(db_filename1):
            os.remove(db_filename1)

        # Re-create the database
        create_database_p2(db_filename1)

        # Data cleaning and processing logic from p2.proceess_data_and_plot_p2
        e_df = clean_excel_data_p2(e_df)

        # Store cleaned data into SQLite database
        insert_dates_to_table_p2(db_filename1, e_df)
        insert_factors_to_table_p2(db_filename1, e_df)
        insert_contributions_to_table_p2(db_filename1, e_df)

    @reactive.Effect
    def init_data():
        print("Initializing data...")
        if not os.path.exists(db_filename1):
            print(f"Database file {db_filename1} does not exist. Creating...")
            create_database_p2(db_filename1)
        else:
            print(f"Database file {db_filename1} already exists.")
        get_pm10_data_reactive.set(get_pm10_data(db_filename1))
        get_all_factors_data_reactive.set(get_all_factors_data(db_filename1))
        print("Data initialization complete.")

    @output
    @render.data_frame
    def processed_pm10_table():
        """
        Display the first 10 rows of fetched pm10 data.
        """
        # Fetch and store processed pm10 data
        return get_pm10_data_reactive().iloc[:10, :]

    @render.download(
        filename=lambda: f"data-{date.today().isoformat()}-{random.randint(100, 999)}.csv"
    )
    def downloadData2():  # 注意这里要与 UI 的 ID 对应
        df_download2 = get_pm10_data_reactive()
        if not df_download2.empty:
            print("Preparing CSV data for download...")
            csv_data = df_download2.to_csv(index=False)
            print("CSV data ready.")
            yield csv_data
        else:
            print("No data available for download.")

    get_all_factors_data_reactive = reactive.Value()

    @output
    @render.data_frame
    def processed_all_factors_table():
        df = get_all_factors_data_reactive()
        if df is not None and not df.empty:
            return df.iloc[:10, :]
        return pd.DataFrame()

    @render.download(
        filename=lambda: f"data-{date.today().isoformat()}-{random.randint(100, 999)}.csv"
    )
    def downloadData3():  # all factors download
        df_download3 = get_all_factors_data_reactive()
        if df_download3 is not None and not df_download3.empty:
            print("Preparing CSV data for download...")
            csv_data = df_download3.to_csv(index=False)
            print("CSV data ready.")
            yield csv_data
        else:
            print("No data available for download.")
            yield ""

    df_pm10 = get_pm10_data_reactive

    @output
    @render.plot
    @reactive.event(input.action_button1)
    def pm10_trend_plot():
        if df_pm10() is None or df_pm10().empty:
            # 返回空白图或警告文本
            fig, ax = plt.subplots(figsize=(6, 3))
            ax.text(
                0.5, 0.5, "No data available!", fontsize=16, ha="center", va="center"
            )
            ax.axis("off")
            return fig

        return plot_pm10_trend(df_pm10())

    @output
    @render.plot
    @reactive.event(input.action_button2)
    def all_factors_trend_plot():
        df_all_factors = get_all_factors_data_reactive()
        if df_all_factors is not None and not df_all_factors.empty:
            print("Data for plotting:", df_all_factors.head(3))
            return plot_all_factors_trend(df_all_factors)
        else:
            print("No valid data available for plotting.")
            return px.line(title="No data available")

    @reactive.Effect
    def log_communication():
        print("Reactive function triggered.")
        print("Current selected element:", input.selected_element())
    
    # Reactivity 处理
    @reactive.Effect
    def update_dropdown():
        """
        动态更新下拉选项。
        """
        df_all_factors = get_all_factors_data_reactive()
        if df_all_factors is None:
            print("No data available for dropdown.")
            return

        options = elements_options(df_all_factors)
        ui.update_selectize("element_dropdown", choices=options)



    @reactive.Effect
    async def handle_submit():
        if input.action_botton3() > 0:
            print("Submit button clicked.")
            selected_element = input.element_dropdown()
            if not selected_element:
                print("No element selected. Please select an element to generate the plot.")
                return

            df_all_factors = get_all_factors_data_reactive()
            if df_all_factors is None or df_all_factors.empty:
                print("df_all_factors is None or empty. Cannot generate plot.")
                return

            print("Reactive data (df_all_factors):")
            print(df_all_factors.head())

            fig = generate_calendar_plot(df_all_factors, selected_element)
            if fig:
                fig.savefig(f"{selected_element}_heatmap.png")
                print("Plot saved successfully.")
            else:
                print("Plot generation returned None.")




    # 1. 核心绘图逻辑：独立的函数
    def generate_calendar_plot(df_all_factors, selected_element):
        # 筛选所需数据
        df_data = df_all_factors[df_all_factors["elements"] == selected_element]
        if df_data.empty:
            print(f"No data available for element: {selected_element}")
            return None

        try:
            # 确保日期列是日期类型
            df_data["date"] = pd.to_datetime(df_data["date"], errors="coerce")
            df_data["year"] = df_data["date"].dt.year
            df_data["month"] = df_data["date"].dt.month

            # 按年和月聚合数据
            df_agg = (
                df_data.groupby(["year", "month"])["contribution_value"]
                .sum()
                .reset_index()
            )

            # 转为透视表
            df_agg_pivot = df_agg.pivot(index="month", columns="year", values="contribution_value")
            df_agg_pivot = df_agg_pivot.fillna(0)  # 填充空值为0

            # 设置画布和颜色映射
            fig, ax = plt.subplots(figsize=(12, 8))
            heatmap = ax.imshow(df_agg_pivot, cmap="YlGnBu", aspect="auto")

            # 添加刻度标签
            ax.set_xticks(np.arange(df_agg_pivot.shape[1]))
            ax.set_yticks(np.arange(df_agg_pivot.shape[0]))
            ax.set_xticklabels(df_agg_pivot.columns, rotation=45)
            ax.set_yticklabels(df_agg_pivot.index)

            # 标注每个单元格
            for i in range(df_agg_pivot.shape[0]):
                for j in range(df_agg_pivot.shape[1]):
                    text = ax.text(
                        j,
                        i,
                        f"{df_agg_pivot.iloc[i, j]:.1f}",
                        ha="center",
                        va="center",
                        color="black",
                        fontsize=8,
                    )

            # 添加标题和色条
            ax.set_title(f"Monthly Heatmap for {selected_element}", fontsize=16)
            ax.set_xlabel("Year", fontsize=12)
            ax.set_ylabel("Month", fontsize=12)
            fig.colorbar(heatmap, ax=ax, label="Contribution Value")

            return fig  # 返回 Matplotlib 图形对象
        except Exception as e:
            print(f"Error during plotting: {e}")
            return None


    # 2. 渲染逻辑：无参数
    @output
    @render.plot
    def plot_calendar_new():
        # 获取反应式数据
        df_all_factors = get_all_factors_data_reactive()
        selected_element = input.element_dropdown()

        if not selected_element or df_all_factors is None or df_all_factors.empty:
            print("No data or element selected.")
            return None  # 避免绘制空内容

        # 调用绘图函数
        fig = generate_calendar_plot(df_all_factors, selected_element)
        if fig is None:
            print("Failed to generate plot.")
            return None

        return fig  # 直接返回 Matplotlib 图形对象





    



    
    



    ######################### P3#############################
    @output
    @render.table
    def preview_table3():
        """
        Render the first 10 rows of the uploaded Excel file for file3.
        """
        if input.file3() is None:
            return pd.DataFrame({"Message": ["No file uploaded yet."]})
        file_info = input.file3()
        uploaded_file_data = pd.read_excel(file_info[0]["datapath"], sheet_name=0)
        return uploaded_file_data.head(10)


app = App(app_ui, server)
