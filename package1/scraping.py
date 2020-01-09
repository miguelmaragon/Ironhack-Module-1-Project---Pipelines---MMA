import pandas as pd
from functools import reduce
from pandas.io.json import json_normalize
import requests
from bs4 import BeautifulSoup
from package1 import module1


def scraping(url1, url2):
    response = requests.get(url1)
    html_soup = BeautifulSoup(response.text, "html.parser")
    results = response.json()
    data_current_all_billionaires = json_normalize(results)
    data_current_all_billionaires = data_current_all_billionaires[
        ['position', 'personName', 'city', 'countryOfCitizenship', 'gender', 'financialAssets', 'birthDate']]
    data_current_all_billionaires.loc[:, 'birthDate'] = data_current_all_billionaires.birthDate.fillna(0)
    data_current_all_billionaires.loc[:, 'birthDate'] = data_current_all_billionaires.birthDate.astype(int)
    module1.title_column(data_current_all_billionaires, 'personName')

    data_current_all_billionaires.loc[:, 'birthDate'] = data_current_all_billionaires.birthDate.apply(
        module1.birthdate_f)
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

    results_merge_data['Age'] = results_merge_data.apply(lambda row: module1.age_none_f(row.Age, row.birthDate), axis=1)
    results_merge_data['Gender'] = results_merge_data.apply(lambda row: module1.gender_none_f(row.Gender, row.gender),
                                                            axis=1)
    results_merge_data['Country'] = results_merge_data.apply(
        lambda row: module1.country_none_f(row.Country, row.countryOfCitizenship), axis=1)

    results_merge_data = results_merge_data[
        ['Position_y', 'Name', 'LastName', 'Age', 'Gender', 'Country', 'city', 'Worth', 'Image', 'Company', 'Sector',
         'financialAssets']]

    results_merge_data.to_csv('./data/processed/ForbesBillionaires2018_url.csv', index=False)

    html = requests.get(url2).content
    soup = BeautifulSoup(html, 'lxml')
    table = soup.find_all('table', {'class': 'stats'})[0]

    data = []
    for i in table.findAll('tr'):
        data.append([j.text.strip() for j in i.findAll('td')])

    data_billionaires_2018_web_scraping = pd.DataFrame(data[2:-3], columns=data[1])
    module1.title_column(data_billionaires_2018_web_scraping, 'Name')
    results = [results_merge_data, data_billionaires_2018_web_scraping]
    results_merge_data_add_info = reduce(lambda left, right: pd.merge(left, right, on='Name', how='left'), results)

    results_merge_data_add_info['Country'] = results_merge_data_add_info.apply(
        lambda row: module1.country_none2_f(row.Country, row.Citizenship), axis=1)
    results_merge_data_add_info = results_merge_data_add_info[
        ['Position_y', 'Name', 'LastName', 'Age_x', 'Gender', 'Country', 'city', 'Worth', 'Image', 'Company', 'Sector',
         'financialAssets']]
    results_merge_data_add_info.rename(
        columns={'Age_x': 'Age', 'Position_y': 'Position', 'city': 'City', 'financialAssets': 'FinancialAssets'},
        inplace=True)
    results_merge_data_add_info = results_merge_data_add_info.sort_values(by='Position', ascending=True)
    results_merge_data_add_info = results_merge_data_add_info.loc[
        results_merge_data_add_info.astype(str).drop_duplicates().index]

    results_merge_data_add_info = results_merge_data_add_info.drop(results_merge_data_add_info[
                                                                       (results_merge_data_add_info.Name == 'Li Li') & (
                                                                               results_merge_data_add_info.City == 'Shenzhen')].index)
    results_merge_data_add_info = results_merge_data_add_info.drop(
        results_merge_data_add_info[
            (results_merge_data_add_info.Name == 'Robert Miller') & (results_merge_data_add_info.City == 'Montreal') & (
                    results_merge_data_add_info.Worth == 4.5)].index)
    results_merge_data_add_info = results_merge_data_add_info.drop(
        results_merge_data_add_info[
            (results_merge_data_add_info.Name == 'Robert Miller') & (
                    results_merge_data_add_info.City == 'Hong Kong') & (
                    results_merge_data_add_info.Worth == 2.6)].index)

    results_merge_data_add_info = results_merge_data_add_info.reset_index(drop=True)

    results_merge_data_add_info.to_csv('./data/results/ForbesBillionaires2018_add_info.csv', index=False)
