import pandas as pd
from sqlalchemy import create_engine
from functools import reduce
from pandas.io.json import json_normalize
import requests
from bs4 import BeautifulSoup
from package1 import module1

sqlitedb_path = './data/raw/mmaragon.db'

engine = create_engine(f'sqlite:///{sqlitedb_path}')

df_sqlite_b_info = pd.read_sql_query("select * from business_info", engine)
df_sqlite_p_info = pd.read_sql_query("select * from personal_info", engine)
df_sqlite_r_info = pd.read_sql_query("select * from rank_info", engine)

df_sqlite_b_info = df_sqlite_b_info.drop(["realTimeWorth", "Unnamed: 0"], axis=1)
df_sqlite_p_info = df_sqlite_p_info.drop(["Unnamed: 0"], axis=1)

"""
def title_column(df, column):
    df[column] = df[column].str.title()
    return df[column]
"""

module1.title_column(df_sqlite_p_info, 'lastName')

df_sqlite_r_info = df_sqlite_r_info.drop(["Unnamed: 0"], axis=1)
module1.title_column(df_sqlite_r_info, 'name')

dfs = [df_sqlite_b_info, df_sqlite_p_info, df_sqlite_r_info]
df_final = reduce(lambda left, right: pd.merge(left, right, on='id'), dfs)

df_final = df_final[['id', 'position', 'name', 'lastName', 'age', 'gender', 'country', 'Source', 'worth', 'image']]
df_final.rename(columns={'id': 'Id', 'position': 'Position', 'name': 'Name', 'lastName': 'LastName', 'age': 'Age',
                         'gender': 'Gender', 'country': 'Country', 'worth': 'Worth', 'image': 'Image'}, inplace=True)

df_final.Position = df_final.Position.astype(int)
df_final = df_final.sort_values(by='Position', ascending=True)

"""
def gender_f(the_row):
    if the_row == "M":
        return "Male"
    elif the_row == "F":
        return "Female"
    else:
        return the_row


def age_f(the_row):
    the_row = int(the_row)  # añadido!!!!
    if the_row >= 1000:
        return now.year - the_row
    else:
        return the_row
"""

df_final['Gender'] = df_final.apply(lambda row: module1.gender_f(row.Gender), axis=1)

df_final['Age'] = df_final.Age.str.replace(' years old', '')
df_final['Age'] = df_final.Age.fillna(0)
df_final['Age'] = df_final.apply(lambda row: module1.age_f(row.Age), axis=1)

df_final['Worth'] = df_final.Worth.str.replace(' BUSD', '')
df_final['Worth'] = df_final.Worth.astype(float)

df_final['Sector'] = df_final.Source.str.split(' ==> ').str[0].str.title()
df_final['Company'] = df_final.Source.str.split(' ==> ').str[1].str.title()
df_final = df_final.drop(["Source"], axis=1)

df_final['Image'] = df_final.Image.str.replace('https:http://', 'https://')
df_final['Image'] = df_final.Image.str.replace('/n', '')

df_final.to_csv('./data/processed/ForbesBillionaires2018.csv', index=False)
print('Proceso terminado')

url = 'https://forbes400.herokuapp.com/api/forbes400/getAllBillionaires'
response = requests.get(url)
html_soup = BeautifulSoup(response.text, "html.parser")
results = response.json()
data_current_all_billionaires = json_normalize(results)
data_current_all_billionaires = data_current_all_billionaires[
    ['position', 'personName', 'city', 'countryOfCitizenship', 'gender', 'financialAssets', 'birthDate']]
data_current_all_billionaires.loc[:, 'birthDate'] = data_current_all_billionaires.birthDate.fillna(0)
data_current_all_billionaires.loc[:, 'birthDate'] = data_current_all_billionaires.birthDate.astype(int)
module1.title_column(data_current_all_billionaires, 'personName')

"""
def birthdate_f(the_row):
    if the_row > 0:
        date = datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=the_row / 1000)
        return date.strftime('%Y')
    elif the_row < 0:
        date = datetime.datetime(1970, 1, 1) - datetime.timedelta(seconds=-the_row / 1000)
        return date.strftime('%Y')
    else:
        return 0
"""

data_current_all_billionaires.loc[:, 'birthDate'] = data_current_all_billionaires.birthDate.apply(module1.birthdate_f)
data_current_all_billionaires.loc[:, 'birthDate'] = data_current_all_billionaires.birthDate.apply(module1.age_f)
data_current_all_billionaires.loc[:, 'gender'] = data_current_all_billionaires.gender.apply(module1.gender_f)
data_current_all_billionaires.rename(index={e: e + 1 for e in range(len(data_current_all_billionaires))},
                                     columns={'personName': 'Name', 'position': 'Position'},
                                     inplace=True)

data_billionaires_2018 = pd.read_csv('./data/processed/ForbesBillionaires2018.csv', sep=',', index_col=0)

results = [data_current_all_billionaires, data_billionaires_2018]
results_merge_data = reduce(lambda left, right: pd.merge(left, right, on='Name', how='right'), results)

results_merge_data.birthDate = results_merge_data.birthDate.fillna(0)
results_merge_data.birthDate = results_merge_data.birthDate.astype(int)

"""
def age_none_f(age, birthDate):
    if age == 0:
        return (birthDate - 1)
    else:
        return age


def gender_none_f(Gender, gender):
    if Gender in (None, '', 'None'):
        return gender
    else:
        return Gender


def country_none_f(Country, countryOfCitizenship):
    if Country in (None, '', 'None'):
        return countryOfCitizenship
    elif Country == 'USA':
        return 'United States'
    else:
        return Country
"""

results_merge_data['Age'] = results_merge_data.apply(lambda row: module1.age_none_f(row.Age, row.birthDate), axis=1)
results_merge_data['Gender'] = results_merge_data.apply(lambda row: module1.gender_none_f(row.Gender, row.gender), axis=1)
results_merge_data['Country'] = results_merge_data.apply(lambda row: module1.country_none_f(row.Country, row.countryOfCitizenship), axis=1)

results_merge_data = results_merge_data[
    ['Position_y', 'Name', 'LastName', 'Age', 'Gender', 'Country', 'city', 'Worth', 'Image', 'Company', 'Sector',
     'financialAssets']]

url = 'https://stats.areppim.com/listes/list_billionairesx18xwor.htm'
html = requests.get(url).content
soup = BeautifulSoup(html, 'lxml')
table = soup.find_all('table', {'class': 'stats'})[0]

data = []
for i in table.findAll('tr'):
    data.append([j.text.strip() for j in i.findAll('td')])

data_billionaires_2018_web_scraping = pd.DataFrame(data[2:-3], columns=data[1])
module1.title_column(data_billionaires_2018_web_scraping, 'Name')
results = [results_merge_data, data_billionaires_2018_web_scraping]
results_merge_data_add_info = reduce(lambda left, right: pd.merge(left, right, on='Name', how='left'), results)

"""
def country_none2_f(Country, Citizenship):
    if pd.isnull(Country) == True:
        return Citizenship
    elif Country == 'USA':
        return 'United States'
    else:
        return Country
"""

results_merge_data_add_info['Country'] = results_merge_data_add_info.apply(lambda row: module1.country_none2_f(row.Country, row.Citizenship), axis=1)
results_merge_data_add_info = results_merge_data_add_info[
    ['Position_y', 'Name', 'LastName', 'Age_x', 'Gender', 'Country', 'city', 'Worth', 'Image', 'Company', 'Sector',
     'financialAssets']]
results_merge_data_add_info.rename(
    columns={'Age_x': 'Age', 'Position_y': 'Position', 'city': 'City', 'financialAssets': 'FinancialAssets'},
    inplace=True)
results_merge_data_add_info = results_merge_data_add_info.sort_values(by='Position', ascending=True)
results_merge_data_add_info = results_merge_data_add_info.loc[results_merge_data_add_info.astype(str).drop_duplicates().index]

results_merge_data_add_info = results_merge_data_add_info.drop(results_merge_data_add_info[(results_merge_data_add_info.Name == 'Li Li') & (results_merge_data_add_info.City == 'Shenzhen')].index)
results_merge_data_add_info = results_merge_data_add_info.drop(
    results_merge_data_add_info[(results_merge_data_add_info.Name == 'Robert Miller') & (results_merge_data_add_info.City == 'Montreal') & (results_merge_data_add_info.Worth == 4.5)].index)
results_merge_data_add_info = results_merge_data_add_info.drop(
    results_merge_data_add_info[(results_merge_data_add_info.Name == 'Robert Miller') & (results_merge_data_add_info.City == 'Hong Kong') & (results_merge_data_add_info.Worth == 2.6)].index)

results_merge_data_add_info = results_merge_data_add_info.reset_index(drop=True)

results_merge_data_add_info.to_csv('./data/processed/ForbesBillionaires2018_add_info.csv', index=False)
print('Proceso terminado')