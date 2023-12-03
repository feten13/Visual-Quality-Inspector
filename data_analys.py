import schedule
import time
import pandas as pd
import matplotlib.pyplot as plt
import datetime

def run_script():
    currentdate = datetime.datetime(2023, 2, 22, 8, 30, 0)
    # print(currentdate.date())
    try:
        qd = pd.read_csv('QDataResultRR.txt', sep='\t', header=None, encoding='utf-16')
    except UnicodeEncodeError:
        qd = pd.read_csv('QDataResultRR.txt', sep='\t', header=None, encoding='utf-8')
    except UnicodeError:
        qd = pd.read_csv('QDataResultRR.txt', sep='\t', header=None, encoding='utf-8')
    df = pd.read_excel('LBT-RL_zsb.xlsx', header=None)
    df = df.iloc[4:].reset_index()
    df.columns = df.iloc[0]
    df = df[1:].reset_index()
    df = df.drop_duplicates(subset=['Operation'])
    columns_to_drop = [4, 'Operation', 'FPNR ', 'Coord 1', 'Bohrpunkt 1', 'Material',
                       'Wire', '  Color', 'Cross', 'Length', 'Coord 2',
                       'Bohrpunkt 2', 'Prozessnummer', 'Prozessdescription\nAktion', 'Gesamt Zeit', 'Take Rates']
    df = df.drop(columns=columns_to_drop)
    df['Cavity 1'] = df['Cavity 1'].fillna(0)
    df['Cavity 2'] = df['Cavity 2'].fillna(0)
    df['Position 2'] = df['Position 2'].fillna(0)
    df = df.dropna(axis=1)
    df = df[df['Cavity 1'] != 0]
    df = df[df['Cavity 2'] != 0]
    shifts_boundaries = [(datetime.time(6, 0, 0), datetime.time(14, 0, 0)),
                         (datetime.time(14, 0, 0), datetime.time(22, 0, 0))]
    shift_start, shift_end = None, None
    shift = []
    for start, end in shifts_boundaries:
        if start <= currentdate.time() < end:
            shift_start = datetime.datetime.combine(currentdate.date(), start)
            shift.append(shift_start)
            shift_end = shift_start + datetime.timedelta(hours=8)
            break
    droped_columns_qd = [1, 2, 10, 11, 12, 13, 14, 15, 5, 7, 8, 9]
    qd = qd.drop(columns=droped_columns_qd)
    qd = qd[qd[6] != "Wackler"]
    qd.reset_index(drop=True, inplace=True)
    qd.rename(
        columns={qd.columns[0]: 'datetime', qd.columns[1]: 'connectors', qd.columns[2]: 'kam',
                 qd.columns[3]: 'type'}, inplace=True)
    if shift_start is not None:
        qd['datetime'] = pd.to_datetime(qd['datetime'], format='%m/%d/%Y %I:%M:%S %p')
        filtred_qd = qd[(qd['datetime'] >= shift_start) & (qd['datetime'] < shift_end)]
    else:
        filtred_qd = qd
    csv_filename = 'filtred_qdd.csv'
    filtred_qd.to_csv(csv_filename, index=False)
    ER_count = filtred_qd.groupby(['connectors', 'kam']).size().reset_index(name='Counts')
    top3ER = ER_count.sort_values(by='Counts', ascending=False).head().reset_index()
    name_ER = top3ER.iloc[:, 1].to_list()
    ex = 1
    my_list = []
    for name in name_ER:
        filtered_rows = filtred_qd[filtred_qd[filtred_qd.columns[1]] == name]
        if not filtered_rows.empty:
            my_list.append(filtered_rows)
        else:
            filtered_rows = qd[qd[qd.columns[1]] == name]
            my_list.append(filtered_rows)
            ex = 0
    sorted_qd = pd.concat(my_list, ignore_index=True)
    grouped = sorted_qd.groupby(sorted_qd.columns[[1, 2, 3]].tolist()).size().reset_index(name='Counts')
    sorted_groups = grouped.sort_values(by='Counts', ascending=False).head()
    sorted_groups.rename(columns={sorted_groups.columns[0]: 'connectors',
                                  sorted_groups.columns[1]: 'Kam',
                                  sorted_groups.columns[2]: 'type'}, inplace=True)
    colors = ['red', 'magenta', 'orange', 'yellow', 'green']
    fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(10, 10))
    plt.subplots_adjust(hspace=0.5)

    plt.subplot(2, 1, 1)
    for color, (_, row) in zip(colors, sorted_groups.iterrows()):
        connectors = row['connectors'] + "\n kam N°" + str((row['Kam']))
        error_type = row['type']
        count = row['Counts']
        bar = plt.bar(connectors, count, align='center', color=color)
        a = 17
        b = 10
        if len(error_type) > a:
            first_part = error_type[:a]
            second_part = error_type[a:a + b]
            third_part = error_type[a + b:]
            formatted_type = f"{first_part}_\n{second_part}_\n{third_part}"
        else:
            formatted_type = error_type
        plt.text(bar[0].get_x() + bar[0].get_width() / 2, bar[0].get_height() + 0.7, formatted_type,
                 ha='center', va='bottom', rotation='horizontal', fontsize=10)
    filtred_df = []
    # sorted_groups.info()
    presence = []
    for _, row in sorted_groups.iterrows():
        kamm = row['Kam']
        conn = row['connectors']
        takt1 = df[(df['Position 1'].astype(str) == str(conn)) & (df['Cavity 1'].astype(str) == str(kamm))]
        if not takt1.empty:
            filtred_df.append(takt1)
        else:
            takt2 = df[(df['Position 1'].astype(str) == str(conn)) & (df['Cavity 1'].astype(str) == str(kamm))]
            if not takt2.empty:
                filtred_df.append(takt2)
            else:
                unfounded_data = {'Position 1': [conn], 'Cavity 1': [kamm],
                                  'Position 2': [conn], 'Cavity 2': [kamm],
                                  'Arbeitsplatznr.': ['99']}
                unfounded_df = pd.DataFrame(unfounded_data)
                filtred_df.append(unfounded_df)
    result = pd.concat(filtred_df)
    result = result.reindex(
        columns=[col for col in result.columns if col != 'Arbeitsplatznr.'] + ['Arbeitsplatznr.'])
    # print('result=\n', result.head())
    column_order = ['Position 1', 'Cavity 1', 'Position 2', 'Cavity 2', 'Arbeitsplatznr.']
    result = result[column_order]
    ############
    position = {'01': [1286, 605], '02': [1160, 605], '03': [1038, 605], '04': [860, 605], '05': [738, 605],
                '00': [1618, 470], '99': [1900, 300]}
    # takt_name = ['takt_1', 'takt_2', 'takt_3', 'takt_4', 'takt_5', 'VOR','MM']
    ##################mapping###################
    plt.subplot(2, 1, 2)
    map_image = plt.imread('Drxmap+++.png')
    plt.imshow(map_image)
    takttot = []
    for i, row in result.iterrows():
        takttot.append(str(row['Arbeitsplatznr.'][:2]))
    c = 0
    for num, color in zip(takttot, colors):
        if num in position:
            x, y = position[num]
            plt.scatter(x, y + c, color=color, s=500, marker='*')
            c = c + 20
    legends = []
    for index, row in result.iterrows():
        test = row['Arbeitsplatznr.']
        name = row['Position 1']
        if test != '99':
            legends.append('taktN°:' + test)
        else:
            if (test == '99') & (str(row['Cavity 1']).startswith('Presence') == True):
                legends.append('Presence Problem: ' + name)
            else:
                if (test == '99') & (str(row['Cavity 1']).startswith('Presence') != True):
                    legends.append('Unfounded check MM: ' + name)
    if ex == 1:
        plt.title('local visualisation per shift', loc='center', pad=0)
    else:
        plt.title('Global visualisation for Qdata ', currentdate.date().year, loc='center', pad=0)

    plt.legend(legends)
    plt.legend(legends, loc='upper left')
    #############################
    columns = ["Timestamp", "duration", "position 1", "Value1", "Value2", "position 2", "Value3", "Value4",
               "Product ID"]
    fauld = pd.read_csv('23_01_04_FaultDataFile.txt', sep='\t', names=columns)
    droped_columns_fauld = ["Value1", "Value2", "Value3", "Value4"]
    fauld = fauld.drop(columns=droped_columns_fauld)
    last_id = fauld.iloc[-1]["Product ID"]
    last_product = []
    for i, row in fauld[::-1].iterrows():
        if row['Product ID'] == last_id:
            last_product.append(row)
        else:
            break
    last_product_df = pd.DataFrame(last_product)
    last_product_df['Timestamp'] = pd.to_datetime(fauld['Timestamp'], format='<< %d.%m.%Y %H:%M:%S >>')
    last_product_df = last_product_df.sort_values(by='duration', ascending=False).head()
    last_product_df['position 1'] = last_product_df['position 1'].fillna('')
    last_product_df['position 2'] = last_product_df['position 2'].fillna('')
    plt.figure(2)
    colors = ['red', 'magenta', 'orange', 'yellow', 'green']
    for (i, row), color in zip(last_product_df.iterrows(), colors):
        test1 = str(row['position 1'])
        a = 17
        b = 10
        if len(test1) > a:
            first_part = test1[:a]
            second_part = test1[a:a + b]
            third_part = test1[a + b:]
            formatted_type = f"{first_part}_\n{second_part}_\n{third_part}"
        else:
            formatted_type = test1

        test = str(formatted_type) + "\n" + str(row['position 2'])
        plt.bar(test, row['duration'], color=color)
        plt.xlabel('top errors per KSK')
        plt.ylabel('duration')
        plt.title(f'Top 3 Slowest Tests for Product {last_id}')
        plt.xticks(rotation='horizontal')
        ########################################

    plt.show()
schedule.every(10).minutes.do(run_script)
while True:
    schedule.run_pending()
    time.sleep(1)