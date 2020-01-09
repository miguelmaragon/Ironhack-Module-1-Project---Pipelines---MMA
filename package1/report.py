import pandas as pd
from package1 import module2
from package1 import module3


def report(results_merge_data_add_info):

    reports = []
    report1 = pd.DataFrame(results_merge_data_add_info.groupby('Sector')['Name'].count().sort_values(ascending=False))
    report1.rename(columns={'Name': 'Count'}, inplace=True)
    report1['Percentage'] = round(report1.Count / sum(report1.Count) * 100, 1)
    reports.append('report1')

    report2 = pd.DataFrame(results_merge_data_add_info.groupby('Gender')['Name'].count().sort_values(ascending=False))
    report2.rename(columns={'Name': 'Count'}, inplace=True)
    report2['Percentage'] = round(report2.Count / sum(report2.Count) * 100, 1)
    reports.append('report2')

    report3 = pd.DataFrame(
        results_merge_data_add_info.loc[results_merge_data_add_info['Age'] != -1, :].groupby('Sector')['Age', 'Worth'].agg(
            ['min', 'max', 'mean', 'median', 'std']))
    reports.append('report3')

    mpg_labels = ['1- Younger: < 38', '2- A little less young: 38 < 53', '3- Moderate: 53 < 68', '4- A little old: 68 < 84',
                  '5- Older 84 < 99']
    report5 = pd.DataFrame(
        pd.cut(results_merge_data_add_info.loc[results_merge_data_add_info['Age'] != -1, 'Age'], bins=5, labels=mpg_labels))
    report5 = pd.DataFrame(report5.groupby('Age')['Age'].count())
    report5.rename(columns={'Age': 'Age_group', 'Age': 'Age'}, inplace=True)
    report5['Percentage'] = round(report5.Age / sum(report5.Age) * 100, 1)
    reports.append('report5')

    image5 = results_merge_data_add_info.loc[results_merge_data_add_info['Age'] != -1, 'Age'].hist(bins=5, figsize=(16, 8))
    fig = image5.get_figure()
    fig.savefig('./reports/image5.png')
    reports.append('image5')

    report6 = pd.DataFrame(results_merge_data_add_info.groupby('Company')['Name'].count().sort_values(ascending=False))
    report6.rename(columns={'Name': 'Count'}, inplace=True)
    report6['Percentage'] = round(report6.Count / sum(report6.Count) * 100, 1)
    report6 = report6.head(10)
    reports.append('report6')

    css = """
        <style type=\"text/css\">
        table {
        color: #333;
        font-family: Helvetica, Arial, sans-serif;
        width: 100%;
        background-color: #FFFFFF;
        border-color: #7ea8f8;
        border-collapse: collapse;
        border-style: solid;
        border-spacing: 0;
        color: #000000;
        }
        td, th {
        border-color: #7ea8f8;
        height: 30px;
        text-align: center;
        }
        th {
        background: #DFDFDF;
        font-weight: bold;
        }
        td {
        background: #FFFFFF;
        text-align: center;
        }
        table tr:nth-child(odd) td{
        background-color: white;
        }
        </style>
        """

    module2.dataframe_to_image(report1, css, './reports/report1.png')
    module2.dataframe_to_image(report2, css, './reports/report2.png')
    module2.dataframe_to_image(report3, css, './reports/report3.png')
    module2.dataframe_to_image(report5, css, './reports/report5.png')
    module2.dataframe_to_image(report6, css, './reports/report6.png')

    module3.to_pdf(reports)