import sys

import numpy as np
import pandas as pd
import time
import matplotlib
import matplotlib.pyplot as plt
from PyQt6 import uic
from PyQt6.QtWidgets import QApplication, QMainWindow, QWidget, QLabel

from urllib3.util.util import to_str


class MyWidget(QMainWindow):
    def __init__(self):
        super().__init__()
        uic.loadUi('./ui/window.ui', self)  # Загружаем дизайн
        self.pushButton.clicked.connect(self.run)

    def run(self):
        test_gr = pd.read_csv('all_test.csv', delimiter=',')
        contr_gr = pd.read_csv('all_contr.csv', delimiter=',')
        list_of_best = ['МГУ', "МГТУ", "МФТИ", "МГИМО", "МИФИ", "СПбГУ", "ВШЭ", "РАНХиГС", "ИТМО", "УрФУ", "РУДН",
                        "МИСИС",
                        "КФУ", "РЭУ", "ФУ", "РЭШ", "РНИМУ", "МГЮА", "ПСПбГУ", "МИРЭА", "СПбГПМУ", "СПбПУ", "МГМСУ",
                        "МГЛУ",
                        "УП РФ", "СПбГЭТУ", "МАИ", "МГПУ", "НМИЦ", "МГМСУ"]
        str_of_best = '|'.join(list_of_best)

        date_parts = test_gr['bdate'].str.split('.', expand=True)
        test_gr['year'] = pd.to_numeric(date_parts[2], errors='coerce')

        final_test = pd.DataFrame()
        mask = (test_gr['year'].isnull() == False) & (test_gr['universities'].isnull() == False) & (
            test_gr['universities'].str.contains(str_of_best, na=False)) & (test_gr['year'] < 2008) & (
                       2002 < test_gr['year'])
        final_test['id'] = test_gr['id'].loc[mask].copy()
        final_test['year'] = test_gr['year'].loc[mask].copy()
        final_test['universities'] = 1
        final_test['year'] = final_test['year'].astype(int)
        final_test['data_from'] = 1

        final_test2 = pd.DataFrame()
        mask2 = (test_gr['year'].isnull() == False) & (test_gr['universities'].isnull() == False) & (
                test_gr['universities'].str.contains(str_of_best, na=False) == False) & (test_gr['year'] < 2008) & (
                        2002 < test_gr['year'])
        final_test2['id'] = test_gr['id'].loc[mask2].copy()
        final_test2['year'] = test_gr['year'].loc[mask2].copy()
        final_test2['universities'] = 0
        final_test2['year'] = final_test2['year'].astype(int)
        final_test2['data_from'] = 1

        final_test = pd.concat([final_test, final_test2], ignore_index=True)

        date_parts_2 = contr_gr['bdate'].str.split('.', expand=True)
        contr_gr['year'] = pd.to_numeric(date_parts_2[2], errors='coerce')

        temp_contr = pd.DataFrame()
        mask_contr = (contr_gr['year'].isnull() != True) & (contr_gr['universities'].isnull() != True) & (
            contr_gr['universities'].str.contains(str_of_best, na=False)) & (contr_gr['year'] < 2008) & (
                             2002 < contr_gr['year']) & ((contr_gr['id'].isin(test_gr['id'])) == False)
        temp_contr['id'] = contr_gr['id'].loc[mask_contr].copy()
        temp_contr['year'] = contr_gr['year'].loc[mask_contr].copy()
        temp_contr['universities'] = 1
        temp_contr['data_from'] = 0
        temp_contr['year'] = temp_contr['year'].astype(int)

        temp_contr2 = pd.DataFrame()
        mask_contr2 = (contr_gr['year'].isnull() != True) & (contr_gr['universities'].isnull() != True) & (
                contr_gr['universities'].str.contains(str_of_best, na=False) != True) & (contr_gr['year'] < 2008) & (
                              2002 < contr_gr['year']) & ((contr_gr['id'].isin(test_gr['id'])) == False)
        temp_contr2['id'] = contr_gr['id'].loc[mask_contr2].copy()
        temp_contr2['year'] = contr_gr['year'].loc[mask_contr2].copy()
        temp_contr2['universities'] = 0
        temp_contr2['data_from'] = 0
        temp_contr2['year'] = temp_contr2['year'].astype(int)
        temp_contr = pd.concat([temp_contr, temp_contr2], ignore_index=True)

        final_test = pd.concat([final_test, temp_contr], ignore_index=True)
        corr_result = final_test[['data_from', 'universities']].corr(method="spearman").iloc[0, 1]

        final_test.to_csv('test.csv', index=False)
        props = final_test.groupby("data_from")["universities"].value_counts(normalize=True).unstack()
        props.plot(kind='bar', stacked=True, color=['lightgrey', 'gold'])
        plt.xlabel('Откуда данные', fontsize=12)
        plt.ylabel('Доля количества данных', fontsize=12)
        plt.title('Доля студентов топ-вузов в группах')
        plt.legend(['Не "Топовый" ВУЗ', '"Топовый ВУЗ"'], loc='lower right')
        plt.show()
        self.label.setText(f'Корреляция равна {corr_result.astype('float')}.\n'
                           f'Это слабая подтверждающая, но \n'
                           f'положительная зависимость')

if __name__ == '__main__':
    app = QApplication(sys.argv)
    ex = MyWidget()
    ex.show()
    sys.exit(app.exec())