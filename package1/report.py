from package1 import module2
from package1 import module3
import pandas as pd
from pandas.io.json import json_normalize
import json
import ast


def report(results_merge_data_add_info):
    reports = []
    report1 = pd.DataFrame(results_merge_data_add_info.groupby('Sector')['Name'].count().sort_values(ascending=False))
    report1.rename(columns={'Name': 'Count'}, inplace=True)
    report1['Percentage'] = round(report1.Count / sum(report1.Count) * 100, 1)
    report1 = report1.reset_index()
    report1.index += 1
    reports.append('report1')

    report2 = pd.DataFrame(results_merge_data_add_info.groupby('Gender')['Name'].count().sort_values(ascending=False))
    report2.rename(columns={'Name': 'Count'}, inplace=True)
    report2['Percentage'] = round(report2.Count / sum(report2.Count) * 100, 1)
    report2 = report2.reset_index()
    report2.index += 1
    reports.append('report2')

    report3 = pd.DataFrame(
        results_merge_data_add_info.loc[results_merge_data_add_info['Age'] != -1, :].groupby('Sector')[
            'Age', 'Worth'].agg(
            ['min', 'max', 'mean', 'median', 'std']))
    reports.append('report3')

    mpg_labels = ['Younger: < 38', 'A little less young: 38 < 53', 'Moderate: 53 < 68', 'A little old: 68 < 84',
                  'Older 84 < 99']
    report5 = pd.DataFrame(pd.cut(results_merge_data_add_info.loc[results_merge_data_add_info['Age'] != -1, 'Age'], bins=5, labels=mpg_labels))
    report5 = pd.DataFrame(report5.groupby('Age')['Age'].count())
    report5.rename(columns={'Age': 'Age_group', 'Age': 'Age_count'}, inplace=True)
    report5['Percentage'] = round(report5.Age_count / sum(report5.Age_count) * 100, 1)
    report5 = report5.reset_index()
    report5.index += 1
    report5.rename(columns={'Age': 'Age_group'}, inplace=True)
    reports.append('report5')

    image5 = results_merge_data_add_info.loc[results_merge_data_add_info['Age'] != -1, 'Age'].hist(bins=5,
                                                                                                   figsize=(16, 8))
    fig = image5.get_figure()
    fig.savefig('./reports/image5.png')
    reports.append('image5')

    report6 = pd.DataFrame(results_merge_data_add_info.groupby('Company')['Name'].count().sort_values(ascending=False))
    report6.rename(columns={'Name': 'Count'}, inplace=True)
    report6['Percentage'] = round(report6.Count / sum(report6.Count) * 100, 1)
    report6 = report6.head(10)
    report6 = report6.reset_index()
    report6.index += 1
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

    table_stokes = results_merge_data_add_info[['Name', 'FinancialAssets']].dropna()

    Ls = []

    def New_DF(Name, FinancialAssets):
        test_string = '"' + FinancialAssets[1:-1] + '"'
        try:
            res = json.loads(test_string)
            result_dict = ast.literal_eval(res)
            if type(result_dict) == dict:
                # Ls = [v for k, v in i.items() if k=='exchange']
                for k, v in i.items():
                    if k == 'exchange':
                        exchange = v
                    elif k == 'companyName':
                        Ls.append(Name + " ~ " + v + " ~ " + exchange)
            else:
                # Ls = [v for i in result_dict for k, v in i.items() if k=='exchange']
                for i in result_dict:
                    for k, v in i.items():
                        if k == 'exchange':
                            exchange = v
                        elif k == 'companyName':
                            Ls.append(Name + " ~ " + v + " ~ " + exchange)
        except:
            return FinancialAssets
        return list(set(Ls))

    table_stokes.apply(lambda row: New_DF(row.Name, row.FinancialAssets), axis=1)

    Ls = []

    table_stokes['more_data'] = table_stokes.apply(lambda row: New_DF(row.Name, row.FinancialAssets), axis=1)

    Ls2 = [i.split(' ~ ') for i in Ls]

    df1 = pd.DataFrame(Ls2)
    df1.drop_duplicates()
    df1.rename(columns={0: 'Name', 1: 'CompanyName', 2: 'Exchange'}, inplace=True)

    report7 = df1[['Name', 'CompanyName', 'Exchange']].drop_duplicates()
    report7 = pd.DataFrame(report7.groupby(['CompanyName', 'Exchange'])['Name'].count().sort_values(ascending=False))
    report7.rename(columns={'Name': 'Count'}, inplace=True)
    report7['Percentage'] = round(report7.Count / sum(report7.Count) * 100, 1)
    report7 = report7.reset_index().head(3)
    report7.index += 1
    report7 = report7[['CompanyName', 'Exchange']]
    reports.append('report7')

    report8 = df1[['Exchange', 'Name']].drop_duplicates()
    report8 = pd.DataFrame(report8.groupby('Exchange')['Name'].count().sort_values(ascending=False))
    report8.rename(columns={'Name': 'Count'}, inplace=True)
    report8['Percentage'] = round(report8.Count / sum(report8.Count) * 100, 1)
    report8 = report8.reset_index().head(3)
    report8.index += 1
    reports.append('report8')

    report9 = df1[['Name', 'CompanyName']].drop_duplicates()
    report9 = pd.DataFrame(report9.groupby('Name')['CompanyName'].count().sort_values(ascending=False))
    report9.rename(columns={'CompanyName': 'Count'}, inplace=True)
    report9 = report9.reset_index().head(3)
    report9.index += 1
    reports.append('report9')

    module2.dataframe_to_image(report1, css, './reports/report1.png')
    module2.dataframe_to_image(report2, css, './reports/report2.png')
    module2.dataframe_to_image(report3, css, './reports/report3.png')
    module2.dataframe_to_image(report5, css, './reports/report5.png')
    module2.dataframe_to_image(report6, css, './reports/report6.png')
    module2.dataframe_to_image(report7, css, './reports/report7.png')
    module2.dataframe_to_image(report8, css, './reports/report8.png')
    module2.dataframe_to_image(report9, css, './reports/report9.png')

    module3.to_pdf(reports)
