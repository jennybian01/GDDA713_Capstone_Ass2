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

app = App(app_ui, server)