import pandas as pd
from package1 import cleaning
from package1 import scraping
from package1 import report
from package1 import emailing

sqlitedb_path = './data/raw/mmaragon.db'

cleaning.clean(sqlitedb_path)
print('First step finished')

url1 = 'https://forbes400.herokuapp.com/api/forbes400/getAllBillionaires'
url2 = 'https://stats.areppim.com/listes/list_billionairesx18xwor.htm'

scraping.scraping(url1, url2)
print('Second step finished')

results_merge_data_add_info = pd.read_csv('./data/results/ForbesBillionaires2018_add_info.csv', sep=',', index_col=0)
report.report(results_merge_data_add_info)
print('Third step finished')

emailing.send('report_all.pdf')
print('Fourth step finished')
