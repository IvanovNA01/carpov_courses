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

#sns.set(rc={'figure.figsize':(19, 13), 'axes.titlesize':16, 'axes.labelsize':14, 'xtick.labelsize':12, 'ytick.labelsize':12,'axes.titlepad': 30 })

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20220220'
}

bot = telegram.Bot(token=os.environ.get("report_bot_token"))
#updates = bot.getUpdates()
#print(updates[-1])

#chat_id = 1295860693
chat_id = -1001735251250

q = 'select toDate(time) as date, user_id, action  from simulator_20220220.feed_actions where toDate(time) > today()-8 and toDate(time) < today()'

df_feed = pandahouse.read_clickhouse(q, connection=connection)

q = 'select toDate(time) as date, user_id, reciever_id  from simulator_20220220.message_actions where toDate(time) > today()-8 and toDate(time) < today()'

df_mess = pandahouse.read_clickhouse(q, connection=connection)

# FEED METRICS
DAU_FEED = df_feed.groupby('date', as_index = False).user_id.nunique().rename(columns={'user_id':'DAU_FEED'})
Likes = df_feed.loc[df_feed.action == 'like'].groupby('date', as_index = False).user_id.count().rename(columns={'user_id':'likes'})
Views = df_feed.loc[df_feed.action == 'view'].groupby('date', as_index = False).user_id.count().rename(columns={'user_id':'views'})
report_metrics = DAU_FEED.merge(Likes).merge(Views)
report_metrics['LPU'] = round(report_metrics.likes/report_metrics.DAU_FEED,2)
report_metrics['VPU'] = round(report_metrics.views/report_metrics.DAU_FEED,2)
report_metrics['CTR_%'] = round(report_metrics.likes*100/report_metrics.views,2)
report_metrics['actions'] = report_metrics.likes+report_metrics.views

# MESSAGE METRICS
MESSAGE = df_mess.groupby('date', as_index = False).user_id.count().rename(columns={'user_id':'message'})
DAU_MESS = df_mess.groupby('date', as_index=False).user_id.nunique().rename(columns={'user_id':'DAU_MESS'})
DAU_FEEDandMESS = df_feed.merge(df_mess, how = 'inner', on = ['date','user_id'])\
                        .groupby('date', as_index=False)\
                        .user_id.nunique()\
                        .rename(columns={'user_id':'DAU_FEEDandMEES'})
report_metrics = report_metrics.merge(DAU_FEEDandMESS).merge(MESSAGE).merge(DAU_MESS)
report_metrics['MPU']=round(report_metrics.message/report_metrics.DAU_MESS,2)

report_metrics = report_metrics.reindex(columns=['date', 'likes', 'views','CTR_%', 'DAU_FEED', 'DAU_MESS','DAU_FEEDandMEES', 'actions', 'LPU','VPU', 'message','MPU'])

#разворачиваем, чтобы 1 была вчерашняя дата
invers_report_metrics = report_metrics.sort_values('date', ascending=False)
#сохраняем данные за вчерашнюю дату
yesterday_report_metrics = invers_report_metrics.iloc[0, :]
#находим процентное отношение остальных дат к вчерашней
invers_report_metrics.iloc[:, 1:] = (invers_report_metrics.iloc[:, 1:] - invers_report_metrics.iloc[0, 1:].values.squeeze())\
                                        .div(invers_report_metrics.iloc[:, 1:])
#восстанавливаем значения за вчера
for i in range(len(yesterday_report_metrics)):
    invers_report_metrics.iloc[0, i] =yesterday_report_metrics[i]
#переводим в проценты и строки
invers_report_metrics.iloc[1:, 1:] = invers_report_metrics.iloc[1:, 1:].multiply(100).astype(float).round(2).astype(str) + '%'
invers_report_metrics['date']=invers_report_metrics['date'].astype(str)

value_metrics = invers_report_metrics.iloc[[0,1,-1], 4:].T.values
name_metrics  = report_metrics.iloc[:, 4:].columns.values
col_Labels = ['yesterday','1 day ago','1 week ago']

# send message to telegram
message = 'Отчет за {v0} по значениям продуктовых метрик по ленте новостей и сообщениям. \nТакже добавлены данные на день и неделю раньше, в процентном отношении от вчерашней даты.'\
        .format(v0 = invers_report_metrics.values[0][0])
message = message + '\nСсылка на основной дашборд <a href=\"https://superset.lab.karpov.courses/superset/dashboard/519/\">Лента новостей и сообщений</a>.'
bot.sendMessage(chat_id=chat_id, text=message, parse_mode = 'HTML')

sns.set(rc={'figure.figsize':(10,8), 'axes.titlesize':16, 'axes.labelsize':14, 'xtick.labelsize':12, 'ytick.labelsize':12,'axes.titlepad': 30 })
#создаем таблицу
fig, ax = plt.subplots() 

plt.title('Таблица значений продуктовых метрик.')
table = ax.table( 
    cellText = value_metrics,  
    rowLabels = name_metrics,  
    colLabels = col_Labels, 
    rowColours =["steelblue"] * 10,  
    colColours =["steelblue"] * 10, 
    cellLoc ='center',  
    loc="center") 
#bbox = [0.2, 0.4, 0.4, 0.02]
#настройка размера ячеек
ax.axis("off") 
table.auto_set_font_size(False)
table.set_fontsize(15) 
table.auto_set_column_width(col=list(range(len(report_metrics.columns))))

for row in range(1, len(name_metrics)+1):
    cell = table[row, -1]
    cell.set_height(0.1)
for col in range(0, len(col_Labels)):
    for row in range(0, len(name_metrics)+1):
        cell = table[row, col]
        cell.set_height(0.1)
plot_object = io.BytesIO()
plt.savefig(plot_object)
plot_object.seek(0)
plot_object.name = 'Table_metrics.png'
plt.close()
bot.sendPhoto(chat_id=chat_id, photo=plot_object)

sns.set(rc={'figure.figsize':(32, 30), 'axes.titlesize':16, 'axes.labelsize':14, 'xtick.labelsize':12, 'ytick.labelsize':12,'axes.titlepad': 30 })
# создание и посылка группы графиков
fig, axs = plt.subplots(4 , 2)
number=0
for i in range(0,len(axs)):
    for j in range(0,len(axs)-2):
        number+=1
        axs[i, j].plot('date', name_metrics[number-1], data = report_metrics)
        axs[i, j].set_title('{v}'.format(v=name_metrics[number-1]))
#добавляет картинку в буфер для посылки        
plot_object = io.BytesIO()
plt.savefig(plot_object)
plot_object.seek(0)
plot_object.name = 'graph_metrics.png'
plt.close()
bot.sendPhoto(chat_id=chat_id, photo=plot_object)

#посылка цсв файла ДФ
file_object = io.StringIO()
report_metrics.to_csv(file_object)
file_object.name = 'report_metrics.csv'
file_object.seek(0)
bot.sendDocument(chat_id=chat_id, document=file_object)