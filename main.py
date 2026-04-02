import pandas as pd
import time
from PyQt6 import uic
from PyQt6.QtWidgets import QApplication, QMainWindow
from PyQt6.QtCore import QThread, pyqtSignal
from PyQt6.QtGui import QPixmap
from dotenv import load_dotenv
import os
import sys
import matplotlib.pyplot as plt
import vk_api

load_dotenv()
token = os.getenv('TOKEN')
vk_session = vk_api.VkApi(token=token)
vk = vk_session.get_api()


class LoaderThread(QThread):
    finished = pyqtSignal(object)
    progress = pyqtSignal(str)

    def __init__(self, group_id_test, group_id_control):
        super().__init__()
        self.group_id_test = group_id_test #group_id_control = ['46936573'] group_id_test = ['141995075', '127973328']
        self.group_id_control = group_id_control

    def run(self):

        members_test = []
        members_control = []

        for id in self.group_id_test:
            offset = 0
            while True:
                try:
                    response = vk.groups.getMembers(group_id=id, fields="bdate, universities", offset=offset)
                    for user in response['items']:
                        if 'universities' in user and user['universities']:
                            uni_name = user['universities'][0].get('name')
                            user['universities'] = uni_name
                    members_test.extend(response['items'])
                    if offset + 1000 >= response['count']:
                        break
                    if offset >= 150000:
                        break
                    self.progress.emit(f"Загружено данных тестовых {len(members_test)}")
                    print(f"Загружено данных тестовых {len(members_test)}")
                    offset += 1000
                    time.sleep(0.5)
                except vk_api.exceptions.ApiError as e:
                    if e.code == 9:
                        self.progress.emit(f"ОШИБКА {e}")
                        print(f"ОШИБКА {e}")
                        time.sleep(60)
                        continue
        for id in self.group_id_control:
            offset = 0
            while True:
                try:
                    response = vk.groups.getMembers(group_id=id, fields="bdate, universities", offset=offset)
                    for user in response['items']:
                        if 'universities' in user and user['universities']:
                            uni_name = user['universities'][0].get('name')
                            user['universities'] = uni_name
                    members_control.extend(response['items'])
                    if offset + 1000 >= response['count']:
                        break
                    if offset >= 150000:
                        break
                    self.progress.emit(f"Загружено данных контрольных {len(members_control)}")
                    print(f"Загружено данных контрольных {len(members_control)}")
                    offset += 1000
                    time.sleep(0.5)
                except vk_api.exceptions.ApiError as e:
                    if e.code == 9:
                        self.progress.emit(f"ОШИБКА {e}")
                        print(f"ОШИБКА {e}")
                        time.sleep(60)
                        continue
        df_control = pd.DataFrame(members_control)
        df_test = pd.DataFrame(members_test)
        df_control.to_csv('all_contr.csv', index=False)
        df_test.to_csv('all_test.csv', index=False)
        result = "Данные успешно загружены"
        self.finished.emit(result)


class MyWidget(QMainWindow):
    def __init__(self):
        super().__init__()
        uic.loadUi('./ui/main.ui', self)  # Загружаем дизайн
        self.corrBtn.clicked.connect(self.run)
        self.downloadBtn.clicked.connect(self.start_loading)

    def start_loading(self):
        test_ids = [i.strip() for i in self.lineEditTest.text().split(',') if i.strip()]
        control_ids = [i.strip() for i in self.lineEditContr.text().split(',') if i.strip()]
        self.downloadBtn.setEnabled(False)  # Отключаем кнопку, чтобы не нажали дважды
        self.thread = LoaderThread(test_ids, control_ids)

        self.thread.progress.connect(self.update_status)

        self.thread.finished.connect(self.on_finished)
        self.thread.start()

    def update_status(self, text):
        self.files_label.setText(text)

    def on_finished(self, result):
        self.files_label.setText(result)
        self.downloadBtn.setEnabled(True)

    def run(self):
        plt.close()
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
        plt.get_current_fig_manager().set_window_title('Результаты анализа групп')
        plt.legend(['Не "Топовый" ВУЗ', '"Топовый ВУЗ"'], loc='lower right')
        plt.show()
        if 0.4 >= corr_result.astype('float') > 0:
            self.corr_label.setText(f'Корреляция равна {corr_result.astype('float')}.\n'
                                    f'Это слабая подтверждающая, но \n'
                                    f'положительная зависимость')
        elif 0.4 < corr_result.astype('float'):
            self.corr_label.setText(f'Корреляция равна {corr_result.astype('float')}.\n'
                                    f'Это подтверждающая, \n'
                                    f'положительная зависимость')
        elif -0.4 <= corr_result.astype('float') < 0:
            self.corr_label.setText(f'Корреляция равна {corr_result.astype('float')}.\n'
                                    f'Это слабая подтверждающая, но \n'
                                    f'отрицательная зависимость')
        elif -0.4 > corr_result.astype('float'):
            self.corr_label.setText(f'Корреляция равна {corr_result.astype('float')}.\n'
                                    f'Это подтверждающая, \n'
                                    f'отрицательная зависимость')


if __name__ == '__main__':
    app = QApplication(sys.argv)
    ex = MyWidget()
    ex.show()
    sys.exit(app.exec())