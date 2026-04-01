import vk_api
import pandas as pd
import time
from PyQt6 import uic
from PyQt6.QtWidgets import QApplication, QMainWindow
from PyQt6.QtCore import QThread, pyqtSignal
from dotenv import load_dotenv
import os
import sys

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
        uic.loadUi('./ui/downloader_window.ui', self)  # Загружаем дизайн
        self.pushButton.clicked.connect(self.start_loading)

    def start_loading(self):
        test_ids = [i.strip() for i in self.lineEdit.text().split(',') if i.strip()]
        control_ids = [i.strip() for i in self.lineEdit_2.text().split(',') if i.strip()]
        self.pushButton.setEnabled(False)  # Отключаем кнопку, чтобы не нажали дважды
        self.thread = LoaderThread(test_ids, control_ids)

        self.thread.progress.connect(self.update_status)

        self.thread.finished.connect(self.on_finished)
        self.thread.start()

    def update_status(self, text):
        self.label_3.setText(text)

    def on_finished(self, result):
        self.label_3.setText(result)
        self.pushButton.setEnabled(True)


if __name__ == '__main__':
    app = QApplication(sys.argv)
    ex = MyWidget()
    ex.show()
    sys.exit(app.exec())