import vk_api
import pandas as pd
import time

token = '63f78eaf63f78eaf63f78eafe860c9688e663f763f78eaf0a6649ed7cba842d03baed32'
group_id_control = '46936573'
group_id_test1 = '141995075'
group_id_test2 = '127973328'


vk_session = vk_api.VkApi(token=token)
vk = vk_session.get_api()

members_control =[]
members_test = []

offset = 0
while True:
    try:
        response = vk.groups.getMembers(group_id=group_id_test1, fields="bdate, universities", offset=offset)
        for user in response['items']:
            if 'universities' in user and user['universities']:
                uni_name = user['universities'][0].get('name')
                user['universities'] = uni_name
        members_test.extend(response['items'])
        if offset + 1000 >= response['count']:
            break
        if offset >= 150000:
            break
        print(f"Загружено данных тестовых {len(members_test)}")
        offset += 1000
        time.sleep(0.5)
    except vk_api.exceptions.ApiError as e:
        if e.code == 9:
            print(f"ОШИБКА")
            time.sleep(60)
            continue

offset = 0
while True:
    try:
        response = vk.groups.getMembers(group_id=group_id_test2, fields="bdate, universities", offset=offset)
        for user in response['items']:
            if 'universities' in user and user['universities']:
                uni_name = user['universities'][0].get('name')
                user['universities'] = uni_name

        members_test.extend(response['items'])
        if offset + 1000 >= response['count']:
            break
        if offset >= 150000:
            break
        print(f"Загружено данных тестовых {len(members_test)}")
        offset += 1000
        time.sleep(0.5)
    except vk_api.exceptions.ApiError as e:
        if e.code == 9:
            print(f"ОШИБКА")
            time.sleep(60)
            continue

offset = 0
while True:
    try:
        response = vk.groups.getMembers(group_id=group_id_control, fields="bdate, universities", offset=offset)
        for user in response['items']:
            if 'universities' in user and user['universities']:
                uni_name = user['universities'][0].get('name')
                user['universities'] = uni_name
        members_control.extend(response['items'])
        if offset + 1000 >= response['count']:
            break
        if offset >= 150000:
            break
        print(f"Загружено данных контрольных {len(members_control)}")
        offset += 1000
        time.sleep(0.5)
    except vk_api.exceptions.ApiError as e:
        if e.code == 9:
            print(f"ОШИБКА")
            time.sleep(60)
            continue

df_control = pd.DataFrame(members_control)
df_test = pd.DataFrame(members_test)
df_control.to_csv('all_contr.csv', index=False)
df_test.to_csv('all_test.csv', index=False)
