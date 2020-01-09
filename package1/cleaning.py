import pandas as pd
from sqlalchemy import create_engine
from functools import reduce
from package1 import module1


def clean(sqlitedb_path):
    engine = create_engine(f'sqlite:///{sqlitedb_path}')

    df_sqlite_b_info = pd.read_sql_query("select * from business_info", engine)
    df_sqlite_p_info = pd.read_sql_query("select * from personal_info", engine)
    df_sqlite_r_info = pd.read_sql_query("select * from rank_info", engine)

    df_sqlite_b_info = df_sqlite_b_info.drop(["realTimeWorth", "Unnamed: 0"], axis=1)
    df_sqlite_p_info = df_sqlite_p_info.drop(["Unnamed: 0"], axis=1)

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