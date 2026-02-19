import numpy as np
import pandas as pd
import time

from urllib3.util.util import to_str

test_gr = pd.read_csv('all_test.csv', delimiter=',')
contr_gr = pd.read_csv('all_contr.csv', delimiter=',')
list_of_best = ['МГУ', "МГТУ", "МФТИ", "МГИМО", "МИФИ", "СПбГУ", "ВШЭ", "РАНХиГС", "ИТМО", "УрФУ", "РУДН", "МИСИС", "КФУ", "РЭУ", "ФУ", "РЭШ", "РНИМУ", "МГЮА", "ПСПбГУ", "МИРЭА", "СПбГПМУ", "СПбПУ", "МГМСУ", "МГЛУ", "УП РФ", "СПбГЭТУ", "МАИ", "МГПУ", "НМИЦ", "МГМСУ"]
data_test = []

date_parts = test_gr['bdate'].str.split('.', expand=True)
test_gr['year'] = pd.to_numeric(date_parts[2], errors='coerce')

mask = (test_gr['year'].isnull() != True) & (test_gr['year'] < 2009) & (2000 < test_gr['year'])
final_test = test_gr['year'].loc[mask].copy()
final_test = final_test.astype(int)


final_test.to_csv('test.csv', index=False)

