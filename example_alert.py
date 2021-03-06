import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import logging
import pandas as pd
import pandahouse
import os
import datetime as dt

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20220220'
}

def check_anomaly(df, metric, threshold=0.3):
    # функция check_anomaly предлагает алгоритм проверки значения на аномальность посредством
    # сравнения интересующего значения со значением в это же время сутки назад
    # при желании алгоритм внутри этой функции можно изменить
    current_ts = df['ts'].max()  # достаем максимальную 15-минутку из датафрейма - ту, которую будем проверять на аномальность
    day_ago_ts = current_ts - pd.DateOffset(days=1)  # достаем такую же 15-минутку сутки назад

    current_value = df[df['ts'] == current_ts][metric].iloc[0] # достаем из датафрейма значение метрики в максимальную 15-минутку
    day_ago_value = df[df['ts'] == day_ago_ts][metric].iloc[0] # достаем из датафрейма значение метрики в такую же 15-минутку сутки назад

    # вычисляем отклонение
    if current_value <= day_ago_value:
        diff = abs(current_value / day_ago_value - 1)
    else:
        diff = abs(day_ago_value / current_value - 1)

    # проверяем больше ли отклонение метрики заданного порога threshold
    # если отклонение больше, то вернем 1, в противном случае 0
    if diff > threshold:
        is_alert = 1
    else:
        is_alert = 0

    return is_alert, current_value, diff


def run_alerts(chat=None):
    chat_id = chat or 1295860693
    bot = telegram.Bot(token='5291500178:AAFQqAVPSG4ad90QdyE-IaZH-Ai8ow2oxFs')
    #bot = telegram.Bot(token=os.environ.get("report_bot_token"))
    # для удобства построения графиков в запрос можно добавить колонки date, hm
    q =  ''' select 
                        Full_time, date, Hour_min,
                        sum(activ_feed_users) as activ_feed_users,
                        sum(likes) as likes,
                        sum(views) as views,
                        sum(activ_mess_users) as activ_mess_users,
                        sum(messages) as messages

                    from
                        (select 
                            all_unique_users.Full_time as Full_time ,
                            all_unique_users.date as date,
                            all_unique_users.Hour_min as Hour_min,
                            feed_table.activ_feed_users as activ_feed_users,
                            feed_table.likes as likes,
                            feed_table.views as views,
                            likes*100/views as CTR,
                            mess_table.activ_mess_users AS activ_mess_users,
                            mess_table.messages as messages
                        from
                            (Select 
                                toStartOfFifteenMinutes(time) as Full_time,
                                toDate(Full_time) as date,
                                formatDateTime(Full_time, '%R') as Hour_min,
                                user_id
                            from simulator_20220220.feed_actions
                            group by Full_time, date, Hour_min, user_id
                            union all
                            Select 
                                toStartOfFifteenMinutes(time) as Full_time,
                                toDate(Full_time) as date,
                                formatDateTime(Full_time, '%R') as Hour_min,
                                user_id
                            from simulator_20220220.message_actions
                            group  by Full_time, date, Hour_min, user_id) as all_unique_users
                        left join
                            (select
                                toStartOfFifteenMinutes(time) as Full_time,
                                toDate(Full_time) as date,
                                formatDateTime(Full_time, '%R') as Hour_min,
                                user_id,
                                count(distinct user_id) as activ_feed_users,
                                countIf(user_id, action = 'like') as likes,
                                countIf(user_id, action = 'view') as views
                            from simulator_20220220.feed_actions
                            group by Full_time, date, Hour_min, user_id) as feed_table 
                        on all_unique_users.user_id = feed_table.user_id and all_unique_users.Full_time = feed_table.Full_time
                        left join
                            (select
                                toStartOfFifteenMinutes(time) as Full_time,
                                toDate(Full_time) as date,
                                formatDateTime(Full_time, '%R') as Hour_min,
                                user_id,
                                count(distinct user_id) as activ_mess_users,
                                count(user_id) as messages
                            from simulator_20220220.message_actions
                            group by Full_time, date, Hour_min, user_id) as mess_table
                        on all_unique_users.user_id = mess_table.user_id and all_unique_users.Full_time = mess_table.Full_time
                        where Full_time >=  today() - 1 and Full_time < toStartOfFifteenMinutes(now()) ) as virtual_table
                    group by Full_time, date, Hour_min
                    order by Full_time '''
    df_allert = pandahouse.read_clickhouse(q, connection=connection)
    
    
    
    
    metric = 'users_lenta'
    is_alert, current_value, diff = check_anomaly(data, metric, threshold=0.1) # проверяем метрику на аномальность алгоритмом, описаным внутри функции check_anomaly()
    if is_alert:
        msg = '''Метрика {metric}:\nтекущее значение = {current_value:.2f}\nотклонение от вчера {diff:.2%}'''.format(metric=metric,
                                                                                                                     current_value=current_value,
                                                                                                                     diff=diff)

        sns.set(rc={'figure.figsize': (16, 10)}) # задаем размер графика
        plt.tight_layout()

        ax = sns.lineplot( # строим линейный график
            data=data.sort_values(by=['date', 'hm']), # задаем датафрейм для графика
            x="hm", y=metric, # указываем названия колонок в датафрейме для x и y
            hue="date" # задаем "группировку" на графике, т е хотим чтобы для каждого значения date была своя линия построена
            )

        for ind, label in enumerate(ax.get_xticklabels()): # этот цикл нужен чтобы разрядить подписи координат по оси Х,
            if ind % 15 == 0:
                label.set_visible(True)
            else:
                label.set_visible(False)
        ax.set(xlabel='time') # задаем имя оси Х
        ax.set(ylabel=metric) # задаем имя оси У

        ax.set_title('{}'.format(metric)) # задае заголовок графика
        ax.set(ylim=(0, None)) # задаем лимит для оси У

        # формируем файловый объект
        plot_object = io.BytesIO()
        ax.figure.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = '{0}.png'.format(metric)
        plt.close()

        # отправляем алерт
        bot.sendMessage(chat_id=chat_id, text=msg)
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)


try:
    run_alerts()
except Exception as e:
    print(e)