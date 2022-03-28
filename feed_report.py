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

bot = telegram.Bot(token = '5291500178:AAFQqAVPSG4ad90QdyE-IaZH-Ai8ow2oxFs')
#updates = bot.getUpdates()
#print(updates[-1])
#chat_id = 1295860693
chat_id = -1001735251250

q = 'select toDate(time) as date, user_id, action  from simulator_20220220.feed_actions where toDate(time) > today()-8 and toDate(time) < today()'

df_feed = pandahouse.read_clickhouse(q, connection=connection)

DAU = df_feed.groupby('date', as_index = False).user_id.nunique().rename(columns={'user_id':'DAU'})
Likes = df_feed.loc[df_feed.action == 'like'].groupby('date', as_index = False).user_id.count().rename(columns={'user_id':'likes'})
Views = df_feed.loc[df_feed.action == 'view'].groupby('date', as_index = False).user_id.count().rename(columns={'user_id':'views'})

report_metrics = DAU.merge(Likes).merge(Views)

report_metrics['CTR_%'] = round(report_metrics.likes*100/report_metrics.views,2)
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

value_metrics = invers_report_metrics.drop([1,2,3,4],0).values
name_metrics = report_metrics.columns.values
row_Labels = ['yesterday','1 day ago','1 week ago']

# send message to telegram
message = 'Отчет за {v0} по значениям продуктовых метрик: \nDAU, likes, views, CTR. \nТакже добавлены данные на день и неделю раньше, в процентном отношении от вчерашней даты.'\
        .format(v0 = value_metrics[0][0])
message = message + '\nСсылка на основные дашборды: \n<a href=\"https://superset.lab.karpov.courses/superset/dashboard/327/\">Лента новостей история</a>, \n<a href=\"https://superset.lab.karpov.courses/superset/dashboard/474/\">Лента новостей оперативный</a>. '
bot.sendMessage(chat_id=chat_id, text=message, parse_mode = 'HTML')

sns.set(rc={'figure.figsize':(10,8), 'axes.titlesize':16, 'axes.labelsize':14, 'xtick.labelsize':12, 'ytick.labelsize':12,'axes.titlepad': 30 })
#создаем таблицу
fig, ax = plt.subplots() 

plt.title('Таблица значений продуктовых метрик за вчера.')
table = ax.table( 
    cellText = value_metrics,  
    rowLabels = row_Labels,  
    colLabels = name_metrics, 
    rowColours =["steelblue"] * 10,  
    colColours =["steelblue"] * 10, 
    cellLoc ='center',  
    loc ='center') 
#настройка размера ячеек
ax.axis("off") 
table.auto_set_font_size(False)
table.set_fontsize(15) 
table.auto_set_column_width(col=list(range(len(report_metrics.columns))))

for row in range(1, len(row_Labels)+1):
    cell = table[row, -1]
    cell.set_height(0.1)
for col in range(0, len(name_metrics)):
    for row in range(0, len(row_Labels)+1):
        cell = table[row, col]
        cell.set_height(0.1)
plot_object = io.BytesIO()
plt.savefig(plot_object)
plot_object.seek(0)
plot_object.name = 'Table_metrics.png'
plt.close()
bot.sendPhoto(chat_id=chat_id, photo=plot_object)

sns.set(rc={'figure.figsize':(19, 13), 'axes.titlesize':16, 'axes.labelsize':14, 'xtick.labelsize':12, 'ytick.labelsize':12,'axes.titlepad': 30 })
# создание и посылка группы графиков
fig, axs = plt.subplots(2, 2)
number=0
for i in range(0,len(axs)):
    for j in range(0,len(axs)):
        number+=1
        axs[i, j].plot('date', name_metrics[number-1], data = report_metrics)
        axs[i, j].set_title('{v}'.format(v=name_metrics[number-1]))
        
plot_object = io.BytesIO()
plt.savefig(plot_object)
plot_object.seek(0)
plot_object.name = 'graph_metrics.png'
plt.close()
bot.sendPhoto(chat_id=chat_id, photo=plot_object)
