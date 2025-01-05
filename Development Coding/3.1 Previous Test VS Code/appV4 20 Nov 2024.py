from shiny import App, render, ui
import pandas as pd
from P1 import process_data_and_plot  # type: ignore # Assuming P1 coding is in a separate file named P1.py
from shiny import ui
from shiny import App, ui, render, req
import openpyxl
from shiny import App, ui

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

app_ui = ui.page_fluid(
    ui.card_header("Auckland Air Quality Monitoring Application",
                  style="background-color: #5081c8; color: white; padding: 60px; text-align: center; font-size: 40px;"),
    
    ui.navset_pill_list(  # <<
        ui.nav_panel("Upload", 
                     "Please upload datasets",
                     ui.input_file("file1", "Choose a file to upload:", multiple=True),
                     ui.output_table("dataPreview")),
        ui.nav_panel("Trend Analysis","Trend analysis",
                     ui.navset_tab(
                         ui.nav_panel("Monthly PM10","Monthly PM10 Contributions Over Time"),
                         ui.nav_panel("Yearly All Factors","Yearly Contributions of All Factors Over Time"),
                     )),
        ui.nav_panel("Visualization", "Pie Chart & Source Contribution Plot",
                    ui.navset_tab(
                        ui.nav_panel("Pie Chart","Source Contribution Pie Chart"),
                        ui.nav_panel("Source Contribution Plot","Source Contribution Plot")
                    )),
        ui.nav_panel("Report", 
                     "Report and download",
                     ui.download_button("downloadReport","Download Report"),
                     ui.output_table("reportPreview")),
        id="tab",
    ),  # <<
    ui.tags.style(css)  # 添加CSS样式
)

def server(input, output, session):
    pass

from shiny import App, ui, render, req
import openpyxl
import pandas as pd

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
    ui.card_header("Auckland Air Quality Monitoring Application",
                   style="background-color: #5081c8; color: white; padding: 60px; text-align: center; font-size: 40px;"),
    
    ui.navset_pill_list(
        ui.nav_panel("Upload", 
                     "Please upload datasets",
                     ui.input_file("file1", "Choose an Excel file to upload: PMF_Raw Date", multiple=True),
                     ui.output_table("dataPreview1"),
                     ui.input_file("file2", "Choose an Excel file to upload: Trend Analysis-PM10", multiple=True),
                     ui.output_table("dataPreview2"),
                     ui.input_file("file3", "Choose an third Excel file to upload: Trend Analysis- Sources", multiple=True),
                     ui.output_table("dataPreview3")),

        ui.nav_panel("Trend Analysis","Trend analysis",
                     ui.navset_tab(
                         ui.nav_panel("Monthly PM10","Monthly PM10 Contributions Over Time"),
                         ui.nav_panel("Yearly All Factors","Yearly Contributions of All Factors Over Time"),
                     )),
        ui.nav_panel("Visualization", "Pie Chart & Source Contribution Plot",
                    ui.navset_tab(
                        ui.nav_panel("Pie Chart","Source Contribution Pie Chart"),
                        ui.nav_panel("Source Contribution Plot","Source Contribution Plot")
                    )),
        ui.nav_panel("Report", 
                     "Report and download",
                     ui.download_button("downloadReport","Download Report"),
                     ui.output_table("reportPreview")),
        id="tab",
    ),
    ui.tags.style(css)  # Add CSS styles
)

def server(input, output, session):
    @output
    @render.table
    def dataPreview1():
        req(input.file1())
        
        file_content = input.file1()
        if file_content:
            excel_path = file_content[0]['datapath']
            df1 = pd.read_excel(excel_path)
            return df1.head(10)  # 默认显示前10行

    @output
    @render.table
    def dataPreview2():
        req(input.file2())
        
        file_content = input.file2()
        if file_content:
            excel_path = file_content[0]['datapath']
            df2 = pd.read_excel(excel_path)
            return df2.head(10)  # 默认显示前10行

    @output
    @render.table
    def dataPreview3():
        req(input.file3())
        
        file_content = input.file3()
        if file_content:
            excel_path = file_content[0]['datapath']
            df3 = pd.read_excel(excel_path)
            return df3.head(10)  # 默认显示前10行
        
    # Trigger P1 branch coding for "Monthly PM10 Contributions Over Time"
    @output
    @render.ui
    def trend_analysis_trigger():
        # When "Monthly PM10 Contributions Over Time" is clicked, run P1 branch coding
        if input.nav_panel_active == "Monthly PM10":
            # Replace this with your P1 branch coding
            print("P1 branch coding triggered: Generating Monthly PM10 Contributions Over Time.")
            # Add any backend functionality or data processing needed for the P1 branch here.

app = App(app_ui, server)


























