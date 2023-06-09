def new_get_data(b, user_id, start_date, end_date, df_loc, current_worker, dict1, dict2):
    if type(df_loc) == int:
        conn = psycopg2.connect(host=os.environ['DATABASE_HOST'], dbname=os.environ['DATABASE_NAME'], user=os.environ['DATABASE_USER'],
                                password=os.environ['DATABASE_PASSWORD'],
                                port=os.environ['DATABASE_PORT'])
        name = "SELECT to_char(location_id, 'FM999') as location_id, location_name FROM locations;"
        df_loc = pd.read_sql(name, conn)
        conn.commit()
        conn.close()

    datetime_obj_start = datetime.strptime(start_date, "%Y-%m-%d")
    start_date = datetime_obj_start.strftime("%d.%m.%Y")
    datetime_obj_end = datetime.strptime(end_date, "%Y-%m-%d")
    end_date = datetime_obj_end.strftime("%d.%m.%Y")
    if type(current_worker) == int:
        df_fired = pd.DataFrame(b.get_all('lists.element.get', params={
            'IBLOCK_TYPE_ID': 'processes',
            'IBLOCK_ID': '56'}))
        df_fired['PROPERTY_100'] = df_fired['PROPERTY_100'].transform(lambda x: list(x.values())[0])
        df_fire = df_fired[df_fired['PROPERTY_100'].isin(user_id)]
        df_fire['PROPERTY_100'] = df_fire['PROPERTY_100'].astype(int)
        df_fire['PROPERTY_656'] = df_fire['PROPERTY_656'].transform(lambda x: list(x.values())[0])
        df_fire = df_fire.drop(
            columns=['ID', 'IBLOCK_ID', 'NAME', 'IBLOCK_SECTION_ID', 'CREATED_BY', 'BP_PUBLISHED', 'CODE',
                     'CREATED_USER_NAME', 'PROPERTY_657'])
        df_fire = df_fire.drop_duplicates()
        df_fire = df_fire.reset_index(drop=True)
        df_fire = df_fire.rename(columns={'PROPERTY_100': 'ID сотрудника', 'PROPERTY_656': 'Дата увольнения'})
        df_user = pd.DataFrame(b.get_all('user.get'))
        df_user = df_user[df_user['ID'].isin(user_id)]
        df_user = df_user.rename(columns={'ID': 'id'})
        df_user['name'] = df_user['LAST_NAME'] + ' ' + df_user['NAME']
        df_user = df_user.reset_index(drop=True)
        df_user = df_user[['id', 'name']]
        current_worker, dict1, dict2, list_day_report_print = data_profile(df_user,df_fire,start_date,end_date)
        current_worker = current_worker.reset_index()
    # Получение всех уникальных проектов из полученного ДФ
    projects = current_worker.loc[current_worker['type_'].isin(['project','project_trip']), 'project'].unique()
    projects = [val.replace('-', '') if val.startswith('-') else val for val in projects]
    projects = list(projects)

    data = []
    if len(projects) != 0:
        # if len(projects) == 1:
        #     projects = projects[0]
        url_ask = 'lists.element.get'
        df_projects = pd.DataFrame(b.get_all(url_ask,
                                             params={
                                                 'IBLOCK_TYPE_ID': 'processes',
                                                 'IBLOCK_ID': '22',
                                                 'FILTER': {'ID': projects}}))
        df_projects.rename(columns={'NAME': 'project_name'}, inplace=True)
        df_projects.rename(columns={'ID': 'project_id'}, inplace=True)
        df_projects = df_projects[['project_id', 'project_name']]
        proj = list(df_projects.to_dict('index').values())

        # Составление data для одного юзера для всех полученных проектов (без учета автопроставлений и отсутствий)
        data = [{'id': i + 1, 'project': {'project_id': proj[i]['project_id'], 'name': proj[i]['project_name']},
                 'hours': [{'date': (datetime_obj_start + timedelta(days=i)).strftime("%d.%m.%Y")} for i in
                           range(0, 7)]} for i in
                range(len(proj))]

        # for i in range(0,len(user_id)):
        for data_i in range(0, len(data)):
            keyword_records = []
            counter = 0

            # new_df - ДФ только по одному проекту (data_i из len(data))
            new_df = current_worker[
                (current_worker['type_'] == 'project') & (
                            current_worker['project'] == data[data_i]['project']['project_id'])]
            new_df = new_df.reset_index(drop=True)
            if new_df.empty:
                # Если попали в этот блок кода, значит существует только project_trip
                new_df = current_worker[
                    (current_worker['type_'] == 'project_trip') & (
                            current_worker['project'] == '-'+data[data_i]['project']['project_id'])]
                # new_df['project'] = new_df['project'].str.replace('-', '')
                new_df = new_df.reset_index(drop=True)
            for day in range(0, 7):
                # new_df - заполнение data для data_i проекта
                keyword = (new_df['project'][0], new_df.columns[day + 2])

                if keyword in dict2[user_id[0]]:
                    #Этот блок для заполнения часов data[data_i]['project']['project_id'] из БД (без учета командировки)
                    keyword_records.append(new_df.columns[day + 2])
                    counter += 1
                    if dict2[user_id[0]][keyword]['id_project'] == str(
                            data[data_i]['project']['project_id']):
                        if data[data_i]['hours'][day]['date'] == dict2[user_id[0]][keyword]['date']:
                            try:
                                data[data_i]['hours'][day]['RT'] = int(dict2[user_id[0]][keyword]['RT'])
                            except:
                                data[data_i]['hours'][day]['RT'] = float(dict2[user_id[0]][keyword]['RT'])
                            try:
                                data[data_i]['hours'][day]['OT'] = int(dict2[user_id[0]][keyword]['OT'])
                            except:
                                data[data_i]['hours'][day]['OT'] = float(dict2[user_id[0]][keyword]['OT'])
                            data[data_i]['hours'][day]['status'] = int(
                                dict2[user_id[0]][keyword]['status'])
                            data[data_i]['hours'][day]['id'] = int(dict2[user_id[0]][keyword]['id'])
                            data[data_i]['hours'][day]['comment'] = dict2[user_id[0]][keyword]['comment']
                            data[data_i]['hours'][day]['location_id'] = int(
                                dict2[user_id[0]][keyword]['location'])
                            data[data_i]['hours'][day]['location'] = \
                            df_loc.loc[df_loc['location_id'] == dict2[user_id[0]][keyword]['location']][
                                'location_name'].iloc[0]
                            data[data_i]['hours'][day]['all_hours'] = \
                            current_worker[current_worker['project'] == 'График'][
                                new_df.columns[day + 2]].iloc[0]
                elif ('-' + new_df['project'][0], new_df.columns[day + 2]) in dict2[user_id[0]]:
                    # Этот блок для заполнения часов data[data_i]['project']['project_id'] из БД (только где командировки)
                    keyword_records.append(new_df.columns[day + 2])
                    counter += 1
                    keyword = ('-' + new_df['project'][0], new_df.columns[day + 2])
                    if dict2[user_id[0]][keyword]['id_project'] == str(
                            data[data_i]['project']['project_id']):
                        if data[data_i]['hours'][day]['date'] == dict2[user_id[0]][keyword]['date']:
                            try:
                                data[data_i]['hours'][day]['RT'] = int(dict2[user_id[0]][keyword]['RT'])
                            except:
                                data[data_i]['hours'][day]['RT'] = float(dict2[user_id[0]][keyword]['RT'])
                            try:
                                data[data_i]['hours'][day]['OT'] = int(dict2[user_id[0]][keyword]['OT'])
                            except:
                                data[data_i]['hours'][day]['OT'] = float(dict2[user_id[0]][keyword]['OT'])
                            data[data_i]['hours'][day]['status'] = int(
                                dict2[user_id[0]][keyword]['status'])
                            data[data_i]['hours'][day]['id'] = int(dict2[user_id[0]][keyword]['id'])
                            data[data_i]['hours'][day]['comment'] = dict2[user_id[0]][keyword]['comment']
                            data[data_i]['hours'][day]['location_id'] = int(
                                dict2[user_id[0]][keyword]['location'])
                            data[data_i]['hours'][day]['location'] = \
                                df_loc.loc[
                                    df_loc['location_id'] == dict2[user_id[0]][keyword]['location']][
                                    'location_name'].iloc[0]
                            data[data_i]['hours'][day]['all_hours'] = \
                                current_worker[current_worker['project'] == 'График'][
                                    new_df.columns[day + 2]].iloc[0]
                else:
                    # Этот блок для заполнения пустых дней (нет ключа в dict2)
                    data[data_i]['hours'][day]['RT'] = 0
                    data[data_i]['hours'][day]['OT'] = 0
                    data[data_i]['hours'][day]['status'] = 0
                    data[data_i]['hours'][day]['location_id'] = ''
                    data[data_i]['hours'][day]['location'] = ''
                    data[data_i]['hours'][day]['id'] = ''
                    data[data_i]['hours'][day]['comment'] = ''
                    data[data_i]['hours'][day]['all_hours'] = \
                    current_worker[current_worker['project'] == 'График'][
                        new_df.columns[day + 2]].iloc[0]

            # Если counter = 0, значит все часы, попавшие в DF - автопроставление
            if (counter == 0) & ('-' not in new_df['project'][0]):
                # Если пришел DF с часами только из автопроставления
                # Автопроставление без командировок
                data[data_i]['project']['name'] = 'Автопроставление: ' + data[data_i]['project']['name']
                for day in range(0, 7):
                    for day_auto in range(0, len(dict1[user_id[0]][data[data_i]['project']['project_id']])):
                        if dict1[user_id[0]][data[data_i]['project']['project_id']][day_auto] == \
                                current_worker.columns[day + 2]:
                            data[data_i]['hours'][day]['RT'] = float(
                                current_worker[current_worker['project'] == new_df['project'][0]][
                                    current_worker.columns[day + 2]].iloc[0])
                            data[data_i]['hours'][day]['status'] = 10
                            break
                        else:
                            data[data_i]['hours'][day]['RT'] = 0
                            data[data_i]['hours'][day]['status'] = 0
                        data[data_i]['hours'][day]['OT'] = 0
                        data[data_i]['hours'][day]['location_id'] = ''
                        data[data_i]['hours'][day]['location'] = ''
                        data[data_i]['hours'][day]['id'] = ''
                        data[data_i]['hours'][day]['comment'] = ''
            elif (counter == 0) & ('-' in new_df['project'][0]):
                # Автопроставление с командировкой
                data[data_i]['project']['name'] = 'Автопроставление: ' + data[data_i]['project']['name']
                for day in range(0, 7):
                    for day_auto in range(0, len(dict1[user_id[0]]['-'+data[data_i]['project']['project_id']])):
                        if dict1[user_id[0]]['-'+data[data_i]['project']['project_id']][day_auto] == \
                                current_worker.columns[day + 2]:
                            data[data_i]['hours'][day]['RT'] = float(
                                current_worker[current_worker['project'] == new_df['project'][0]][
                                    current_worker.columns[day + 2]].iloc[0])
                            data[data_i]['hours'][day]['status'] = 10
                            data[data_i]['hours'][day]['location'] = 'Командировка'
                            break
                        else:
                            data[data_i]['hours'][day]['RT'] = 0
                            data[data_i]['hours'][day]['status'] = 0
                            data[data_i]['hours'][day]['location'] = ''
                        data[data_i]['hours'][day]['OT'] = 0
                        data[data_i]['hours'][day]['location_id'] = ''
                        data[data_i]['hours'][day]['id'] = ''
                        data[data_i]['hours'][day]['comment'] = ''
            if (new_df['project'][0] in dict1[user_id[0]]) & (counter != 0) & ('-' not in new_df['project'][0]):
                # Если пришел DF с часами из БД + автопроставление (на один проект)
                # Добавление проекта в data; Автопроставление без командировок;
                checker = 0
                for day in range(0, 7):
                    for day_auto in range(0, len(dict1[user_id[0]][data[data_i]['project']['project_id']])):
                        if (dict1[user_id[0]][data[data_i]['project']['project_id']][day_auto] ==
                            current_worker.columns[day + 2]) & (
                                dict1[user_id[0]][data[data_i]['project']['project_id']][
                                    day_auto] not in keyword_records) & (checker == 0):
                            data.append(
                                {'id': len(data) + 1, 'project': {'name': 'Автопроставление: ' + data[data_i]['project']['name']},
                                 'hours': [{'date': (datetime_obj_start + timedelta(days=i)).strftime("%d.%m.%Y")} for i in
                                           range(0, 7)]})
                            checker = 1
                            for day in range(0, 7):
                                for day_auto in range(0, len(dict1[user_id[0]][data[data_i]['project']['project_id']])):
                                    print('column name: ', current_worker.columns[day + 2], ', dict_auto_date: ',
                                          dict1[user_id[0]][data[data_i]['project']['project_id']][day_auto])
                                    data[len(data) - 1]['hours'][day]['OT'] = 0
                                    data[len(data) - 1]['hours'][day]['location_id'] = ''
                                    data[len(data) - 1]['hours'][day]['id'] = ''
                                    data[len(data) - 1]['hours'][day]['comment'] = ''
                                    if (dict1[user_id[0]][data[data_i]['project']['project_id']][day_auto] ==
                                        current_worker.columns[day + 2]) & (
                                            dict1[user_id[0]][data[data_i]['project']['project_id']][
                                                day_auto] not in keyword_records):
                                        data[len(data) - 1]['hours'][day]['RT'] = float(
                                            current_worker[current_worker['project'] == new_df['project'][0]][
                                                current_worker.columns[day + 2]].iloc[0])
                                        data[len(data) - 1]['hours'][day]['status'] = 10
                                        data[len(data) - 1]['hours'][day]['location'] = ''
                                        break
                                    elif (dict1[user_id[0]][data[data_i]['project']['project_id']][day_auto] ==
                                          current_worker.columns[day + 2]) & (
                                            dict1[user_id[0]][data[data_i]['project']['project_id']][
                                                day_auto] in keyword_records):
                                        data[len(data) - 1]['hours'][day]['RT'] = 0
                                        data[len(data) - 1]['hours'][day]['status'] = 10
                                        data[len(data) - 1]['hours'][day]['location'] = ''
                                        break
                                    else:
                                        data[len(data) - 1]['hours'][day]['RT'] = 0
                                        data[len(data) - 1]['hours'][day]['status'] = 0
            elif ('-' + new_df['project'][0] in dict1[user_id[0]]) & (counter != 0):
                # Если пришел DF с часами из БД + автопроставление (на один проект)
                # Добавление проекта в data; Автопроставление с командировкой
                checker = 0
                for day in range(0, 7):
                    for day_auto in range(0, len(dict1[user_id[0]]['-'+data[data_i]['project']['project_id']])):
                        if (dict1[user_id[0]][data[data_i]['project']['project_id']][day_auto] ==
                            current_worker.columns[day + 2]) & (
                                dict1[user_id[0]]['-'+data[data_i]['project']['project_id']][
                                    day_auto] not in keyword_records) & (checker == 0):
                            data.append(
                                {'id': len(data) + 1,
                                 'project': {'name': 'Автопроставление: ' + data[data_i]['project']['name']},
                                 'hours': [{'date': (datetime_obj_start + timedelta(days=i)).strftime("%d.%m.%Y")} for i
                                           in
                                           range(0, 7)]})
                            checker = 1
                            if '-' + data[data_i]['project']['project_id'] in dict1[user_id[0]]:
                                if (dict1[user_id[0]]['-' + data[data_i]['project']['project_id']][day_auto] ==
                                    current_worker.columns[day + 2]) & (
                                        dict1[user_id[0]][data[data_i]['project']['project_id']][
                                            day_auto] in keyword_records):
                                    data[len(data) - 1]['hours'][day]['RT'] = float(
                                        current_worker[
                                            current_worker['project'] == '-' + data[data_i]['project'][
                                                'project_id']][
                                            current_worker.columns[day + 2]].iloc[0])
                                    data[len(data) - 1]['hours'][day]['status'] = 10
                                    data[len(data) - 1]['hours'][day]['location'] = 'Командировка'
                                    break
                                else:
                                    data[len(data) - 1]['hours'][day]['RT'] = 0
                                    data[len(data) - 1]['hours'][day]['status'] = 0
                                    data[len(data) - 1]['hours'][day]['location'] = ''
                                    break
    if len(current_worker[current_worker['type_'] == 'absence']) != 0:
        # Добавление отсутствия в data
        df_absence = current_worker[current_worker['type_'] == 'absence']
        df_absence = df_absence.reset_index(drop=True)

        for i in range(0, len(df_absence['project'])):
            data.append({'id': len(data) + 1, 'project': {'name': df_absence['project'][i]},
                         'hours': [{'date': (datetime_obj_start + timedelta(days=i)).strftime("%d.%m.%Y")} for i in
                                   range(0, 7)]})
            for day in range(0, 7):
                if df_absence[df_absence.columns[day + 2]].iloc[i] != 0.0:
                    data[len(data) - 1]['hours'][day]['RT'] = df_absence[df_absence.columns[day + 2]].iloc[i]
                    data[len(data) - 1]['hours'][day]['status'] = 10
                else:
                    data[len(data) - 1]['hours'][day]['RT'] = 0
                    data[len(data) - 1]['hours'][day]['status'] = 0
                data[len(data) - 1]['hours'][day]['OT'] = 0
                data[len(data) - 1]['hours'][day]['location_id'] = ''
                data[len(data) - 1]['hours'][day]['location'] = ''
                data[len(data) - 1]['hours'][day]['id'] = ''
                data[len(data) - 1]['hours'][day]['comment'] = ''
    return data