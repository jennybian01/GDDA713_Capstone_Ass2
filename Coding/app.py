from shiny import App, ui, render, req, reactive
import pandas as pd
import os
import sqlite3
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
)
from datetime import date
import random

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
                    ui.input_file("file1", "Choose an Excel file to upload, eg. RUN5PM10_base.xlsx", multiple=False),
                    ui.output_table("preview_table1")  # Ensure this matches the server function
                ),
                ui.nav_panel(
                    "Data processing",
                    "Cleaning and processing dataset.",
                    ui.h3("Total PM10 group by date"),
                    ui.output_data_frame("processed_contributions"),  # Output processed contributions data
                    ui.download_button("downloadData","Download Data")
                ),
                ui.nav_panel("Trend analysis", "Cleaned data trend analysis")
            )
        ),
        ui.nav_panel(
            "Sources Analysis",
            "Trend analysis",
            ui.navset_tab(
                ui.nav_panel(
                    "Monthly PM10",
                    "Monthly PM10 Contributions Over Time",
                    ui.input_file("file2", "Choose an Excel file to upload", multiple=False),
                    ui.output_table("preview_table2")  # Preview for file2
                ),
                ui.nav_panel("Data processing", "Cleaning and processing dataset."),
                ui.nav_panel("Trend Analysis", "Cleaned data trend analysis")
            )
        ),
        ui.nav_panel(
            "Source Contribution",
            "Pie Chart & Source Contribution Plot",
            ui.navset_tab(
                ui.nav_panel(
                    "Upload",
                    "Please upload excel file.",
                    ui.input_file("file3", "Choose an Excel file to upload", multiple=False),
                    ui.output_table("preview_table3")  # Preview for file3
                ),
                ui.nav_panel("Data processing", "Cleaning and processing dataset"),
                ui.nav_panel("Pie Chart", "Source Contribution Pie Chart"),
                ui.nav_panel("Source Contribution Plot", "Source Contribution Plot")
            )
        ),
        id="tab"
    ),
    ui.tags.style(css)  # Add CSS styles
)

# Reactive variable to store the fetched contributions data
contributions_data = reactive.Value(pd.DataFrame())

def server(input, output, session):
    db_filename = "contributions_data.sqlite"

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
        contributions_data.set(fetch_contributions(db_filename))
        # Return the first 10 rows of the fetched data
        return contributions_data().iloc[:10, :]
    
    @render.download(filename=lambda: f"data-{date.today().isoformat()}-{random.randint(100, 999)}.csv")
    def downloadData():
        df_download1=contributions_data()
        if not df_download1.empty:
            csv_data=df_download1.to_csv(index=False)
            yield csv_data

    

    def preview_table2():
        """
        Render the first 10 rows of the uploaded Excel file for file2.
        """
        if input.file2() is None:
            return pd.DataFrame({"Message": ["No file uploaded yet."]})
        file_info = input.file2()
        uploaded_file_data = pd.read_excel(file_info[0]["datapath"], sheet_name=0)
        return uploaded_file_data.head(10)

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

