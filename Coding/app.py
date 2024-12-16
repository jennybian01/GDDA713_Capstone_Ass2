from shiny import App, ui, render, req, reactive
from shiny import session
import pandas as pd
import os
import numpy as np
import sqlite3
import tempfile
import io
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
import logging
from shiny.types import SilentException
from starlette.responses import StreamingResponse
from starlette.responses import JSONResponse
from matplotlib.ticker import FuncFormatter
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
from p3.process_data_and_plot_p3 import(
    clean_data,
    process_dfs_p3,
    pm_10_percentage,
    source_contribution_transformation,
    create_database_p3,
    insert_sources_to_database,
    insert_species_to_database,
    insert_measurements_to_database,
    insert_pm10_percentage_to_db,
    fetch_and_format_data,
)

#File paths
db_filename = "contributions_data.sqlite"
db_filename1 = "elements_data.sqlite"
db_filename2 = "pollution_data.sqlite"

 # Define CSS styles
css = """
body {
    font-family: 'Arial', sans-serif;
    background-color: #fcfcfc; /* 背景颜色 */
    margin: 0;
    padding: 0;
    color: #1E3D51;
}

#tab {
    width: 90%; 
    margin: 0 auto; /* 居中对齐 */
    background-color: #04314b; /* 背景颜色 */
    padding: 20px; /* 内边距 */
    border-radius: 20px; /* 圆角 */
    box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1); /* 阴影效果 */
}

#tab .nav-item {
    background-color: #04314b; /* 背景颜色 */
    padding: 30px;
    border-radius: 30px;
    margin-bottom: 50px;
    text-align: center;
    font-size: 16px; 
}

#tab .nav-link {
    color: #fcfcfd;
    font-weight: bold; 
}

#tab .nav-link.active {
    background-color: #91ad56;
    color: #fcfcfd; /* 激活状态文字颜色 */
}

#tab .tab-content {
    background-color: #7e919e;
    padding: 5px;
    border-radius: 20px;
}

#tab {
    width: 95%; /* 整体宽度 */
    margin: 0 auto; /* 居中对齐 */
}

#tab .nav-pills {
    display: flex;
    flex-wrap: nowrap; /* 如果 pill 太多，可以换行 */
    justify-content: center; /* 居中对齐 pill */
    gap: 10px; /* pill 间距 */
}

#tab .nav-pills .nav-link {
    background-color: #7e919e; /* unactive pill backgroud colour */
    color: #7e919e; /* unactive word colour */
    border-radius: 15px;
    padding: 15px 20px; /* pill size */
    font-size: 30px; /* font size */
}

#tab .nav-pills .nav-link.active {
    background-color: #5081c8; /* 激活状态背景色 */
    color: white; /* 激活状态文字颜色 */
}

#tab .nav-item {
    flex: 0 0 auto; /* 禁止 pill 拉伸 */
    margin: 5px; /* 每个 pill 的外部间距 */
}

.container {
    background-color: transparent;
}

#NZSE_logo {
    display: block;
    max-width: 100%;
    height: auto;
    margin: 0 auto;
}


"""






# Define the UI
app_ui = ui.page_fluid(
    ui.div(
        ui.output_image("image",width="3000px", height="auto"),
        style="position: absolute; top: 10px; left: 20px; width: 3000px; height: auto;"
    ),
    ui.card_header(
        "",
        style="position: relative; padding: 30px; padding-left: 400px;font-size: 45px; font-family: 'YogaSans', sans-serif; font-weight: bold;"
    ),
    ui.card_header(
        ui.div(
            ui.h1("PM Insights Pro", style="font-size: 45px; font-weight: color: #color: #FFFFFF;"),
            ui.h2("Automation (PM10) Speciation Analysis Application", style="font-size: 18px; color: #91ad57; margin-top: 10px;"),  # 添加了 margin-top 以分隔两个标题
            style="background-color: #04314b; padding: 30px; color: #FFFFFF;text-align: center;"
        ),
        style="padding: 10px;"  # 为 header 添加内边距
    ),
    ui.navset_pill_list(
        ui.nav_panel(
            "PM10 Trend Analysis",
            ui.navset_tab(
                ui.nav_panel(
                    "Upload",
                    ui.input_file(
                        "file1",
                        "Please upload an Excel file, for example, 'RUN5PM10_base.xlsx'.",
                        multiple=False,
                    ),
                    ui.output_table("preview_table1"),
                ),
                ui.nav_panel(
                    "Data processing",
                    "Click the button to download the cleaned data.",
                    ui.h6("Total PM10 group by date"),
                    ui.output_data_frame("processed_contributions"),
                    ui.download_button("downloadData", "Download"),
                ),
                ui.nav_panel(
                    "Trend analysis",
                    ui.output_plot("monthly_pm10_trend_plot"),
                ),
            ),
        ),
        ui.nav_panel(
            "Element Trend Analysis",
            "The dataset provides a comprehensive trend analysis of PM10 and its constituent elements, offering insights into air quality patterns over time.",
            ui.navset_tab(
                ui.nav_panel(
                    "Upload",
                    ui.input_file(
                        "file2",
                        "Please upload an Excel file that contains PM10 and elements data, for example, 'QueenStreetPM10PMFDataUncert_May2023.xlsx'.",
                        multiple=False,
                    ),
                    ui.output_table("preview_table2"),
                ),
                ui.nav_panel(
                    "Data processing(PM10)",
                    "Click the button to download the cleaned data.",
                    ui.h6("Total PM10 by date"),
                    ui.output_data_frame("processed_pm10_table"),
                    ui.download_button("downloadData2", "Download Data"),
                ),
                ui.nav_panel(
                    "Data processing(All elements)",
                    "Click the button to download the cleaned data.",
                    ui.h6("Elements group by date"),
                    ui.output_data_frame("processed_all_factors_table"),
                    ui.download_button("downloadData3", "Download Data"),
                ),
                ui.nav_panel(
                    "Trend Analysis(PM10)",
                    "Visualize the PM10 values spanning multiple years by clicking the button.",
                    ui.input_action_button("action_button1", "Visualization"),
                    ui.output_plot("pm10_trend_plot"),
                ),
                ui.nav_panel(
                    "Trend Analysis(All elements)",
                    "Click the button to visualize the values of all elements over the years.",
                    ui.input_action_button("action_button2", "Visualization"),
                    ui.output_plot("all_factors_trend_plot"),
                ),
                ui.nav_panel(
                    "Calendar Plot",
                    "Choose an element to view its trend on a calendar plot.",
                    ui.input_selectize(id="element_dropdown", label="Select an Element:", choices=[]),
                    ui.output_text_verbatim("selected_option"),
                    ui.output_plot("plot_calendar_new"),
                ),
            ),
        ),
        ui.nav_panel(
            "Pollution Contribution",
            "The dataset offers detailed insights into sources and elemental contributions.",
            ui.navset_tab(
                ui.nav_panel(
                    "Upload",
                    ui.input_file(
                        "file3", "Please upload an Excel file that contains sources and elemental contributions data, for example,RUN5PM10_BaseErrorEstimationSummary", multiple=False
                    ),
                    ui.output_table("preview_table3"),
                ),
                ui.nav_panel("Data processing", "Click the button to download the cleaned data.",
                             ui.h6("Pollution Contribution"),
                             ui.output_data_frame("processed_p3_table"),
                             ui.download_button("downloadData4", "Download Data")),
                ui.nav_panel("Pie Chart", "Visualize the sources contribution pie chart by clicking the button.",
                             ui.input_action_button("action_button3", "Visualization"),
                             ui.output_plot("plot_pie_chart")),
                ui.nav_panel("Elements Contribution Plot", "Visualize the elements contribution plot by clicking the button.",
                             ui.input_action_button("action_button4", "Visualization"),
                             ui.output_plot("plot_source_contribution")),
            ),
        ),
        ui.nav_panel("About", 
                     ui.h1("About PM Insights Pro", style="color: #04314b;  text-align: center;"), 
                         ui.accordion(  
                                    ui.accordion_panel("Problem Statement", "Air pollution significantly impacts public health, yet monitoring and analyzing its effects is often hindered by the complexity and time-consuming nature of processing large environmental datasets. Auckland City Council researchers face challenges in performing particulate matter (PM10) speciation analysis, which requires breaking down samples into chemical components to identify pollution sources. Current manual processes are prone to inefficiencies and errors, highlighting the need for a scalable and automated solution."),  
                                    ui.accordion_panel("Purpose and Features", "To enhance the efficiency and accuracy of PM10 speciation analysis at monitoring station through an automated workflow. This project leverages Python and Shiny to streamline data processing, mining, and visualization, empowering researchers to make informed decisions for pollution management and public health policy development. And it features 1) Automated Data Processing; 2) Advanced Data Visualization; 3) User-Friendly Interface; 4) Scalability; 5) Report Generation.By addressing these challenges, the solution transforms labor-intensive processes into streamlined workflows, ultimately supporting Auckland City Council in developing effective environmental strategies and robust public health policies."),  
                                    ui.accordion_panel("How to use?","To use the app, start by uploading your data through the Upload tab. The app is designed specifically for three predefined datasets; please ensure you upload the correct dataset. If an incorrect dataset is uploaded, the app will not respond. For each new upload, please refresh the page before proceeding. The app will automatically clean the data, which will then appear in the Data Processing tab, where you can also download the cleaned data for other purposes. Next, explore the Visualizations tab to view the processed data in graphical formats. Some visualizations require you to click a button to generate the charts, making it easy to analyze and interpret the data."),
                                    ui.accordion_panel("Dataset Information", "The dataset used for this app includes publicly available data provided by the Auckland City Council, specifically focusing on Auckland Queen Street trends for the years 2006 to 2022. However, this app can handling same structure dataset from multiple stations."),  
                                    ui.accordion_panel("Team Information", "Our team, consisting of Wanli Chen and Jenny Bian, is proud to be working on an automated air quality data analysis application as part of a collaborative project between NZSE and the Auckland City Council. This project aims to streamline the processing and analysis of air quality data, providing valuable insights for better environmental management. Guided by our esteemed mentors, Dr. Sara Zandi and Dr. Louis Boamponsem, we have developed an innovative app that automates data cleaning, trend analysis, and visualization. Our goal is to create a user-friendly tool that contributes to informed decision-making for improving Auckland’s air quality."),  
                                    ui.accordion_panel("Contact Information", "For technical support, you are welcome to contact insightspronz@gmail.com."),
                                    ui.accordion_panel("Privacy Policy and Terms", "1) All information used in the app is provided by Auckland City Council and is publicly available; 2) The app itself does not store any information. 3)The app is designed exclusively for use by Auckland City Council. "),   
                                    id="acc",  
                                    open="Section A",  
                                    ),  
                     
                     ),
        id="tab",
        widths=(3, 8),
    ),

    ui.card_footer(
    ui.div(
        ui.output_image("NZSE_logo"),  # 保留 logo
        style="background-color: #f6f6f6; padding: 10px; text-align: center; height: 80px;"  # 简单样式
    )
),




    ui.tags.style(css),  # Add CSS styles
)


dropdown_options = reactive.Value(elements_options)
get_p3_data_reactive= reactive.Value()

def server(input, output, session):
    print("Registered outputs:", dir(output))

    @render.image
    def image():
        from pathlib import Path

        dir = Path(__file__).resolve().parent
        img = {"src": str(dir / "aucklandcouncil.png"), "width": "100px"}
        return img


  
  
    @render.image
    def NZSE_logo():
        from pathlib import Path

        dir = Path(__file__).resolve().parent
        img = {"src": str(dir / "NZSE-logo.png"), "width": "100px"}
        return img


    # Reactive variable to store the fetched contributions data
    contributions_data = reactive.Value(pd.DataFrame())

    # Reactive variable to store the fetched pm10_df and all factors
    get_pm10_data_reactive = reactive.Value()
    get_all_factors_data_reactive = reactive.Value()
    uploaded_data = reactive.Value(None)



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

    # Initialize an empty dataframe, only set after file upload
    df_data = None

    # Initialization function
    def initialize_data():
        global df_data
        # This will only fetch data once a file is uploaded
        data = get_all_factors_data_reactive() or {"date": [], "elements": [], "contribution_value": []}
        df_data = pd.DataFrame(data)

    # Create a reactive value to store the state of the file upload
    file_uploaded = reactive.Value(False)  # This will track if a file is uploaded

    # Create a function to set the file upload state
    def handle_file_upload(file):
        if file:  # If the file exists, mark as uploaded
            file_uploaded.set(True)

    
    
    # Reactive Effect that initializes data only after file is uploaded
    @reactive.Effect
    def init_data():
        # Only initialize data if the file is uploaded
        if file_uploaded.get():
            print("Initializing data...")

            if not os.path.exists(db_filename1):
                print(f"Database file {db_filename1} does not exist. Creating...")
                create_database_p2(db_filename1)
            else:
                print(f"Database file {db_filename1} already exists.")
            
            # Set the data into reactive values after database creation
            get_pm10_data_reactive.set(get_pm10_data(db_filename1))
            get_all_factors_data_reactive.set(get_all_factors_data(db_filename1))
            print("Data initialization complete.")
        else:
            print("File not uploaded yet. Waiting for upload...")


    @output
    @render.table
    def preview_table2():
        """
        Render the first 10 rows of the uploaded Excel file for file2.
        """
        if input.file2() is None:
            return pd.DataFrame({"Message": ["No file uploaded yet."]})
        
        file_info = input.file2()
        uploaded_file_data = pd.read_excel(file_info[0]["datapath"], sheet_name="PMFData")
        return uploaded_file_data.head(10)

    # Reactively trigger the data processing after the file is uploaded
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
                return

            # Delete existing database file if exists, to avoid conflict
            if os.path.exists(db_filename1):
                os.remove(db_filename1)

            # Re-create the database
            create_database_p2(db_filename1)

            # Data cleaning and processing logic from process_data_and_plot_p2
            e_df = clean_excel_data_p2(e_df)

            # Store cleaned data into SQLite database
            insert_dates_to_table_p2(db_filename1, e_df)
            insert_factors_to_table_p2(db_filename1, e_df)
            insert_contributions_to_table_p2(db_filename1, e_df)

            # Re-fetch the cleaned data into reactive values
            get_pm10_data_reactive.set(get_pm10_data(db_filename1))
            get_all_factors_data_reactive.set(get_all_factors_data(db_filename1))

            print("Data processing completed.")



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
    def downloadData2():  
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
    
    # Reactivity 
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
    # 配置 logger
    logger = logging.getLogger(__name__)


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
    


    @reactive.Effect
    @reactive.event(input.file3)
    def process_and_store_data_with_file3():
        logger.info("Triggered process_and_store_data_with_file3 with input.file3.")

        try:
            # Step 1: Validate and read uploaded file
            file_info = input.file3()
            if not file_info:
                logger.error("No file3 uploaded.")
                return

            file_path = file_info[0].get("datapath")
            if not file_path or not os.path.exists(file_path):
                logger.error(f"File not found at path: {file_path}")
                return

            logger.debug(f"File path: {file_path}")
            df = pd.read_excel(file_path)
            logger.info("File successfully read into DataFrame.")

            # Step 2: Create database
            create_database_p3(db_filename2)
            logger.info("Database created.")

            # Step 3: Clean data
            new_dfs = clean_data(df)
            logger.info("Data cleaning completed.")

            # Step 4: Process data
            selected_dfs = process_dfs_p3(new_dfs)
            logger.info("Data processing completed.")

            
            # Step 5: Insert PM10 percentages
            pm10_percentage_df = pm_10_percentage(selected_dfs)  # Pass transformed_dfs as argument
            insert_pm10_percentage_to_db(db_filename2, pm10_percentage_df)
            logger.info("PM10 percentage data inserted into database.")

            # Step 6: Transform source contributions
            transformed_dfs = source_contribution_transformation(selected_dfs)
            logger.info("Source contribution transformation completed.")

            # Step 7: Insert data into database
            insert_sources_to_database(db_filename2, transformed_dfs)
            logger.info("Sources data inserted into database.")

            insert_species_to_database(db_filename2, transformed_dfs)
            logger.info("Species data inserted into database.")

            insert_measurements_to_database(db_filename2, transformed_dfs)
            logger.info("Measurements data inserted into database.")


            # Step 8: Fetch and format data for reactive updates
            formatted_data = fetch_and_format_data(db_filename2)
            get_p3_data_reactive.set(formatted_data)
            logger.info("Reactive data has been set.")

        except Exception as e:
            logger.error(f"Error in process_and_store_data_with_file3: {e}", exc_info=True)





    @output
    @render.data_frame
    def processed_p3_table():
        """
        Render the processed data, if available.
        """
        data = get_p3_data_reactive.get()
        if data is None or data.empty:
            print("No data available to display.")
            return pd.DataFrame({"Message": ["No data available"]})
        return data




    @render.download(
        filename=lambda: f"data-{date.today().isoformat()}-{random.randint(100, 999)}.csv"
    )
    def downloadData4():
        df_download4 = get_p3_data_reactive.get()
        if not df_download4.empty:
            csv_data = df_download4.to_csv(index=False)
            yield csv_data





    @output
    @render.plot
    @reactive.event(input.action_button3)
    def plot_pie_chart():
        """
        绘制显示污染来源 PM10 百分比的饼图。

        参数:
        - db_filename2: str, SQLite 数据库路径。

        返回:
        - fig: matplotlib.figure.Figure, 包含饼图的图像对象。
        """
        print("Starting plot_pie_chart...")  # 调试起点
        try:
            print(f"Attempting to connect to database: {db_filename2}")
            conn = sqlite3.connect(db_filename2)
            cursor = conn.cursor()
            print("Database connection established.")

            # 查询数据
            query = """
            SELECT SourceName, Percentage 
            FROM PollutionSources 
            JOIN Pm10Percentage ON PollutionSources.SourceID = Pm10Percentage.SourceID
            """
            print(f"Executing query: {query}")
            cursor.execute(query)
            data = cursor.fetchall()
            print(f"Fetched data: {data}")

            if not data:
                print("No data retrieved from the database.")
                return None

            # 拆分数据
            labels = [row[0] for row in data]
            sizes = [row[1] for row in data]
            print(f"Labels: {labels}, Sizes: {sizes}")

            # 绘制饼图
            print("Creating pie chart...")
            fig, ax = plt.subplots(figsize=(10, 7), facecolor=(1, 1, 1, 0.5))
            explode = [0.1] * len(labels)
            ax.pie(
                sizes, explode=explode, labels=labels, autopct='%1.1f%%',
                startangle=140, pctdistance=0.85
            )
            ax.axis('equal')
            ax.legend(labels, loc='upper right', bbox_to_anchor=(1.2, 1))
            ax.set_title('Pollution Sources Breakdown', fontsize=16, pad=24)
            print("Pie chart created successfully.")

            return fig

        except sqlite3.Error as e:
            print(f"SQLite error: {e}")
            return None

        except Exception as e:
            print(f"Unexpected error: {e}")
            return None

        finally:
            if 'conn' in locals():
                conn.close()
                print("Database connection closed.")


    @output
    @render.plot
    @reactive.event(input.action_button4)
    def plot_source_contribution():
        """
        Plots a detailed contribution of pollution sources across different species.

        Parameters:
        - db_path: str, the path to the SQLite database.

        Returns:
        - fig: matplotlib.figure.Figure, the figure object containing the plots.
        """
        # Connect to SQLite database
        conn = sqlite3.connect(db_filename2)
        cursor = conn.cursor()

        try:
            # Fetch dynamic data from database
            # Get all sources
            cursor.execute("SELECT SourceID, SourceName FROM PollutionSources")
            sources = cursor.fetchall()

            # Get all species
            cursor.execute("SELECT SpeciesID, Species FROM Species")
            species_data = cursor.fetchall()

            # Build species mapping and order
            species_ids = [row[0] for row in species_data]
            species_names = {row[0]: row[1] for row in species_data}

            # Initialize data structure
            data = {}

            # Query data for each SourceID
            for source_id, source_name in sources:
                cursor.execute("""
                SELECT SpeciesID, Concentration, Average, Error, Dispersion, Exceedance
                FROM Measurement
                WHERE SourceID = ?
                """, (source_id,))
                data[source_id] = {
                    "name": source_name,
                    "values": cursor.fetchall()
                }

        finally:
            # Close the database connection
            conn.close()

        # Plot setup,缩小90%的比例
        original_figsize = (10, len(sources) * 2)  # 原始大小
        reduced_figsize = (original_figsize[0] * 0.55, original_figsize[1] * 0.55)  # 缩小60%
        fig, axes = plt.subplots(len(sources), 1, figsize=reduced_figsize, sharex=True)
        if len(sources) == 1:
            axes = [axes]  # Ensure axes is iterable when there's only one source

        x = np.arange(len(species_ids))  # Label locations
        width = 0.75  # Width of the bars
        fontsize = 8  # 设置y轴字体大小

        # To keep track of whether to add the legend
        first_plot = True

        # Create handles for the legend to ensure only one set of legend items
        legend_handles = []

        for i, (source_id, source_info) in enumerate(data.items()):
            ax = axes[i]
            source_name = source_info["name"]
            source_data = source_info["values"]

            # Extract data for plotting
            species_values = {row[0]: row[1:] for row in source_data}
            concentration = [species_values.get(sid, (0, 0, 0, 0, 0))[0] for sid in species_ids]
            average = [species_values.get(sid, (0, 0, 0, 0, 0))[1] for sid in species_ids]
            error = [species_values.get(sid, (0, 0, 0, 0, 0))[2] for sid in species_ids]
            dispersion = [species_values.get(sid, (0, 0, 0, 0, 0))[3] for sid in species_ids]
            exceedance = [species_values.get(sid, (0, 0, 0, 0, 0))[4] for sid in species_ids]

            # Logarithmic y-axis for concentration
            ax.set_yscale('log')
            ax.set_ylim([1e-4, 1])

            # Format left y-axis ticks to display as scientific notation (10^0, 10^-2, etc.)
            ax.yaxis.set_major_formatter(FuncFormatter(lambda y, _: f'$10^{{{int(np.log10(y))}}}$'))

            # Right y-axis (percentage scale) for Exceedance
            ax2 = ax.twinx()
            ax2.set_ylim([0, 100])
            ax2.set_yticks(np.arange(0, 101, 20))

            # 设置y轴字体大小
            ax.tick_params(axis='y', labelsize=fontsize)
            ax2.tick_params(axis='y', labelsize=fontsize)

            # Background vertical lines
            for j in x:
                ax.vlines(j, 1e-4, 1, colors='gray', linestyles='dashed', lw=0.5)

            # Concentration bars
            bars = ax.bar(x, concentration, width, color='lightblue', edgecolor='black')

            # Error and Dispersion as vertical lines
            line = []
            for j in range(len(x)):
                line.append(ax.plot([x[j], x[j]], [error[j], dispersion[j]], color='black', lw=1))

            # Average as hollow dots
            avg_line = ax.plot(x, average, color='red', marker='o', markersize=6, linestyle='', markerfacecolor='none')

            # Exceedance as green dots
            exceedance_line = ax2.plot(x, exceedance, color='green', marker='o', markersize=6, linestyle='')

            # Add labels to the legend only for the first plot (for each source)
            if first_plot:
                # Create handles for the legend
                legend_handles.append(bars[0])  # The light blue bars (Concentration)
                legend_handles.append(avg_line[0])  # The red hollow dots (Average)
                legend_handles.append(exceedance_line[0])  # The green dots (Exceedance)
                legend_handles.append(line[0][0])  # The black lines (Maximum and minimum DISP values)

                first_plot = False

            # Set title for each subplot (top-right corner)
            ax.text(1, 0.8, source_name, transform=ax.transAxes, ha='right', va='bottom', fontsize=10, weight='bold')

            # Set the x-axis labels only for the last subplot
            if i == len(sources) - 1:
                ax.set_xticks(x)
                ax.set_xticklabels([species_names[sid] for sid in species_ids], rotation=0)
                ax.tick_params(axis='x', labelbottom=True)  # Only show x-axis labels for the last subplot
            else:
                ax.tick_params(axis='x', labelbottom=False)  # Hide x-axis labels for other subplots

        # Add the legend on the left side of the entire figure (only once)
        fig.legend(legend_handles, ['Concentration', 'Average', 'Exceedance', 'Maximum and Minimum DISP values'], 
                loc='upper center', bbox_to_anchor=(0.5, 1.03), ncol=4, frameon=False)

        # Adjust the layout to make space for the legend
        plt.tight_layout()

        return fig


app = App(app_ui, server)
