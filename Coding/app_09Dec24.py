from flask import session
from matplotlib import pyplot as plt
from openpyxl import load_workbook
from param import output
from shiny import App, ui, render, req, reactive
from shiny import App, ui
import pandas as pd
import os
import sqlite3
import tempfile
import stat
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
from datetime import date, datetime
import random
from p2.process_data_and_plot_p2 import(
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
)
from p3.process_data_and_plot_p3 import (
    copy_speciation_column, 
    create_output_file_new, 
    data_preparation, 
    create_database_p3,
    drop_database_p3, 
    get_pollution_contribution,
    insert_Measurement_p3, 
    insert_Species_p3, 
    process_data,
)
import p3.process_data_and_plot_p3 as p3
import pandas as pd
import plotly.express as px
from shiny import ui, render

# Initialize data storage as a reactive variable
data_storage = reactive.Value(None)

# Define CSS styles
css = """
#tab {
    width: 100%; /* 设置导航栏宽度为页面的1/3 */
}
#tab .nav-item {
    background-color: #f9f9f9;
    padding: 20px;
    border-radius: 15px;
    margin-bottom: 20px;
    text-align: center; /* 文字居中 */
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
        style="background-color: #5081c8; color: white; padding: 60px; text-align: center; font-size: 40px;"
    ),
    ui.navset_pill_list(
        ui.nav_panel(
            "Contributions Trend Analysis",
            ui.navset_tab(
                ui.nav_panel(
                    "Upload",
                    "Please upload excel file.",
                    ui.input_file("file1", "Choose an Excel file to upload, eg. RUN3PM10_base.xlsx", multiple=False),
                    ui.output_table("preview_table1")  # Ensure this matches the server function
                ),
                ui.nav_panel(
                    "Data processing",
                    "Cleaning and processing dataset.",                 
                    ui.h5("Total PM10 group by date"),
                    ui.output_data_frame("processed_contributions"),  # Output processed contributions data
                    ui.download_button("downloadData1","Download Data")
                ),
                ui.nav_panel("Trend analysis", 
                             "Cleaned data trend analysis",
                    ui.output_plot("monthly_pm10_trend_plot")    
                             )
            )
        ),
        
        ui.nav_panel(
            "Element Trend Analysis",
            "Trend analysis of each element",
            ui.navset_tab(
                ui.nav_panel(
                    "Upload",
                    "Please upload excel file.",
                    ui.input_file("file2", "Choose an Excel file to upload, eg.QueenStreetPM10PMFDataUncert_May2023.xlsx ", multiple=False),
                    ui.output_table("preview_table2")  # Preview for file2
                            ),
                ui.nav_panel("Data processing(PM10)", "Cleaning and processing dataset.",
                             ui.h5("Total PM10 by date"),
                             ui.output_data_frame("processed_pm10_table"),
                             ui.download_button("downloadData2","Download Data")
                             ),
                ui.nav_panel("Data processing(All elements)", "Cleaning and processing dataset.",
                             ui.h5("Elements group by date"),
                             ui.output_data_frame("processed_all_factors_table"),
                             ui.download_button("downloadData3","Download Data")
                             ),
                ui.nav_panel("Trend Analysis(PM10)", "Cleaned data trend analysis",
                            ui.output_image("pm10_trend_image")
                             ),
                ui.nav_panel("Trend Analysis(All elements)", "Cleaned data trend analysis",
                             ui.output_image("all_factors_trend_image")                             
                             )
            )
        ),
        ui.nav_panel(
            "Pullotion Contribution",
            "Pullotion Contribution Plot and source pie chart",
            ui.navset_tab(
                ui.nav_panel(
                    "Upload",
                    "Please upload an Excel file.",
                    ui.input_file("file3", "Choose an Excel file to upload", multiple=False),   
                    ui.output_table("preview_table3")  # Preview for file3
                ),
                ui.nav_panel(
                            "Pollution Contribution Data processing",
                            "Cleaning and processing dataset",
                            ui.h5("Pollution Contribution"),
                            ui.output_data_frame("processed_pollution_contribution"),   
                            ui.download_button("downloadData4", "Download Data")   
                            ),
                ui.nav_panel(
                            "Pollution Sources Data processing",
                            "Cleaning and processing dataset",
                            ui.h5("Pollution Sources"),
                            ui.output_data_frame("processed_pollution_sources"), 
                            ui.download_button("downloadData5", "Download Data")   
                            ),
                ui.nav_panel(
                            "Pollution Contribution",
                            "Pollution Contribution Plot",
                            ui.output_image("pollution_contribution_image")   
                            ),
                ui.nav_panel(
                            "Pollution Source",
                            "Pollution Source Pie Chart",
                            ui.output_image("pollution_source_image")  
                            )
            ),

)
    )
)
ui.tags.style(css)  # Add CSS styles
id="tab",

# Reactive variable to store the fetched contributions data
contributions_data = reactive.Value(pd.DataFrame())

#Reactive variable to store the fetched pm10_df and all factors
get_pm10_data_reactive= reactive.Value()
get_all_factors_data_reactive=reactive.Value()

# Initialize reactive values pollution contribution
pollution_contribution_data = reactive.Value(pd.DataFrame())
get_pollution_contribution_reactive = reactive.Value()

get_pollution_sources_reactive = reactive.Value()

# Fetch and set pollution contrubution data (simulating database call)
db_filename2 = "pollution_data.sqlite"
pollution_data = get_pollution_contribution(db_filename2)
get_pollution_contribution_reactive.set(pollution_data)  # Set the reactive value


def server(input, output, session):
    db_filename = "contributions_data.sqlite"
    db_filename1= "elements_data.sqlite"
    db_filename2 = "pollution_data.sqlite"

    # Reactive variable to store the uploaded data
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
        contributions_data.set(fetch_contributions(db_filename)) # type: ignore
        # Return the first 10 rows of the fetched data
        return contributions_data().iloc[:10, :] # type: ignore
    
    @render.download(filename=lambda: f"data-{date.today().isoformat()}-{random.randint(100, 999)}.csv")
    def downloadData():
        df_download1=contributions_data() # type: ignore
        if not df_download1.empty:
            csv_data=df_download1.to_csv(index=False)
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
        if input.file3() is None:
            return pd.DataFrame({"Message": ["No file uploaded yet."]})
        file_info = input.file3()
        uploaded_file_data = pd.read_excel(file_info[0]["datapath"], sheet_name="PMFData")
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
        e_df= clean_excel_data_p2(e_df)

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

    @render.download(filename=lambda: f"data-{date.today().isoformat()}-{random.randint(100, 999)}.csv")
    def downloadData2():  # 注意这里要与 UI 的 ID 对应
        df_download2 = get_pm10_data_reactive()
        if not df_download2.empty:
            print("Preparing CSV data for download...")
            csv_data = df_download2.to_csv(index=False)
            print("CSV data ready.")
            yield csv_data
        else:
            print("No data available for download.")

    @output
    @render.data_frame
    def processed_all_factors_table():
        '''
        Display first 10 rows of fetch all factors data
        '''
        #fetch and store all factors data
        return get_all_factors_data_reactive().iloc[:10, :]   
    
    @render.download(filename=lambda: f"data-{date.today().isoformat()}-{random.randint(100, 999)}.csv")
    def downloadData3():  # all factors download
        df_download3 = get_all_factors_data_reactive()
        if not df_download3.empty:
            print("Preparing CSV data for download...")
            csv_data = df_download3.to_csv(index=False)
            print("CSV data ready.")
            yield csv_data
        else:
            print("No data available for download.")

    @output
    @render.image
    def pm10_trend_image():
        try:
            df_pm10 = get_pm10_data_reactive()
            if not df_pm10.empty:
                output_file = tempfile.NamedTemporaryFile(suffix=".png", delete=False).name
                plot_pm10_trend(df_pm10, output_file)
                print(f"Generated trend image at {output_file}")
                return {"src": output_file}
            else:
                print("No data available for plotting.")
                return {"src": ""}
        except Exception as e:
            print(f"Error generating trend image: {e}")
            return {"src": ""}


    @output
    @render.image
    def all_factors_trend_image():
        try:
            # Fetch the factors data
            df_all_factors = get_all_factors_data_reactive()
    
            if not df_all_factors.empty:
                # Specify the directory and file name for the plot
                output_file = "/Users/wanlichen/capstone/p2/trend_image.png"
                output_dir = os.path.dirname(output_file)
            
                    # Ensure the directory exists
                if not os.path.exists(output_dir):
                        os.makedirs(output_dir)
            
                    # Generate the trend plot
                plot_all_factors_trend(df_all_factors, output_file)
            
                    # Check if the file was created successfully
                if not os.path.exists(output_dir):
                    os.makedirs(output_dir)
                plot_all_factors_trend(df_all_factors, output_file)
                return {"src": output_file}
            else:
                print("No data available for plotting.")
            return {"src": ""}
        except Exception as e:
            print(f"Error generating trend image: {e}")
            return {"src": ""}

######################### P3#############################
    # Define the database filename
    db_filename2 = "pollution_data.sqlite"

    # Initialize and Create the Database
    def init_database():
        """
        Initialize the database by creating tables if they don't exist.
        """
        print(f"Initializing database: {db_filename2}")
        create_database_p3(db_filename2)
        print("Database initialized.")

    # Call the database initialization function during startup
    init_database()

    # Example of fetching pollution contribution data
    def get_pollution_contribution(db_filename):
        conn = sqlite3.connect(db_filename)
        query = "SELECT * FROM Measurements"
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df

    # Reactive function using db_filename2
    @reactive.Effect
    def fetch_pollution_data():
        global db_filename2
        pollution_data = get_pollution_contribution(db_filename2)
        get_pollution_contribution_reactive.set(pollution_data)

    @output
    @render.table
    def preview_table3():
        """
        Render the first 10 rows of the uploaded Excel file for file2.
        """
        if input.file2() is None:
            return pd.DataFrame({"Message": ["No file uploaded yet."]})
        file_info = input.file2()
        uploaded_file_data = pd.read_excel(file_info[0]["datapath"], sheet_name="Error Est. Summary_1")
        return uploaded_file_data.head(10)
    
    @ui.reactive.Effect
    def process_and_store():
        
        uploaded_file = input.file3()
        if uploaded_file is None:
            print("No file uploaded. Please upload a file.")
            return

        input_file_path = uploaded_file["input_file3"]
        output_file_path = create_output_file_new("output_file_new.xlsx")
        output_file_new = output_file_path
        input_file_path = input_file_path[0]["datapath"]

        # Copy speciation columns and process data
        wb_new, ws_new = p3.copy_speciation_column("output_file.xlsx", output_file_new)
        process_data("output_file.xlsx", output_file_new, wb_new, ws_new)

        # Data preparation
        df_processed = data_preparation(pd.DataFrame)
        
        # Database handling
        db_filename2 = "database_file.db"
        drop_database_p3(db_filename2)
        create_database_p3(db_filename2)
        insert_Species_p3(db_filename2, df_processed)
        insert_Measurement_p3(db_filename2, df_processed)
        
        # Update reactive data
        pollution_data = get_pollution_contribution(db_filename2)
        get_pollution_contribution_reactive.set(pollution_data)
        
        print("Data processing and storage complete.")

    # Step 3: Initialize data
    @reactive.Effect
    def init_data():
        print("Initializing data...")
        if not os.path.exists(db_filename2):
            print(f"Database file {db_filename2} does not exist. Creating...")
            create_database_p3(db_filename2)
        else:
            print(f"Database file {db_filename2} already exists.")
        pollution_data = get_pollution_contribution(db_filename2)
        get_pollution_contribution_reactive.set(pollution_data)
        print("Data initialization complete.")

    # Step 4: Display Processed Data
    @output
    @render.data_frame
    def processed_pollution_contribution_table():
        """
        Display the first 10 rows of fetched pollution contribution data.
        """
        df = get_pollution_contribution_reactive.get()
        return df.iloc[:10, :] if df is not None else pd.DataFrame()
    
    # Step 5: Download Processed Table in CSV Format
    @render.download(filename=lambda: f"data-{date.today().isoformat()}-{random.randint(100, 999)}.csv")
    def download_data3():
        df_download3 = get_pollution_contribution_reactive.get()  # Fetch the reactive data
        if not df_download3.empty:
            print("Preparing CSV data for download...")
            csv_data = df_download3.to_csv(index=False)  # Convert the data to CSV
            print("CSV data ready.")
            yield csv_data  # Yield the CSV data for download
        else:
            print("No data available for download.")


    # step 6: Display Image (Plot Pollution Contribution)
    @output
    @render.image
    def pollution_contribution_image():
        try:
            df_pollution = get_pollution_contribution_reactive.get()
            if df_pollution is not None and not df_pollution.empty:
                output_file = tempfile.NamedTemporaryFile(suffix=".png", delete=False).name
                # Plot the data (using plotly or any other plotting library)
                plot_pollution_contribution(df_pollution, output_file)
                print(f"Generated trend image at {output_file}")
                return {"src": output_file}
            else:
                print("No data available for plotting.")
                return {"src": ""}
        except Exception as e:
            print(f"Error generating trend image: {e}")
            return {"src": ""}

    # Function to plot pollution contribution (using plotly as an example)
    def plot_pollution_contribution(df_pollution, output_file):
        fig = px.line(df_pollution, x="date", y="pollution_value", title="Pollution Contribution Over Time")
        fig.write_image(output_file)

    app = App(app_ui, server)

    if __name__ == "__main__":
        app.run()

