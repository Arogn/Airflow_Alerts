import pandahouse
from datetime import datetime, timedelta
import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

#параметры, которые прокидываются в таски 
default_args = {
    'owner': 'arogn',
    'depends_on_past': False,
    'retries': 1, #сколько раз повторять таск, если он упал 
    'retry_delay': timedelta(minutes=5), #промежуток между повторными запусками 
    'start_date': datetime(2023, 3, 22),
}

#cron-выражение, говорящее о том, что надо запускать DAG каждые 15 минут 
schedule_interval = '*/15 * * * *'

#check_anomaly - это функция, в которой описан алгоритм поиска аномалий методом межквартильного размаха
def check_anomaly(df, metric, a=3, n=5):
    df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
    df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
    df['iqr'] = df['q75'] - df['q25']
    df['up'] = df['q75'] + a * df['iqr']
    df['low'] = df['q25'] - a * df['iqr']

    df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
    df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()

    if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0

    return is_alert, df


#run_alerts - это функция, которая будет запускать систему алёртов
def run_alerts(chat=None):

    #словарь-коннектор к БД для сбора данных (удалены так как база данных приватна) 
    connection = {
        'host': '',
        'password': '',
        'user': '',
        'database': ''
    }

    #в базе данных хранится две таблицы - таблица по ленте новостей и таблица по мессенджеру 

    chat_id = chat or ''
    bot = telegram.Bot(token='')

    #запрос для получения данных по количеству уникальных пользователей, лайков, просмотров,
    #ctr за каждую 15-ти минутку вчерашнего дня из таблицы по ленте новостей
    q = ''' SELECT
                  toStartOfFifteenMinutes(time) as ts
                , toDate(ts) as date
                , formatDateTime(ts, '%R') as hm
                , uniqExact(user_id) as users_feed
                , countIf(user_id, action='view') as views
                , countIf(user_id, action='like') as likes
                , likes/views as ctr
            FROM tables.f_a
            WHERE ts >=  today() - 1 and ts < toStartOfFifteenMinutes(now())
            GROUP BY ts, date, hm
            ORDER BY ts '''

    #запрос для получения данных по количеству отправленных сообщений за каждую 15-ти
    #минутку вчерашнего дня из таблицы по мессенджеру
    q2 = ''' SELECT
                  toStartOfFifteenMinutes(time) as ts,
                  toDate(ts) as date,
                  formatDateTime(ts, '%R') as hm,
                  count(user_id) as message_count
            FROM tables.m_a
            WHERE ts >= today() - 1 and ts < toStartOfFifteenMinutes(now())
            GROUP BY ts, date, hm
            ORDER BY ts '''

    df = pandahouse.read_clickhouse(q, connection=connection)

    df_mess = pandahouse.read_clickhouse(q2, connection=connection)

    print(df)
    print(df_mess)

    #metrics_list - это перечень метрик для мониторинга из таблицы по ленте новостей
    metrics_list = ['users_feed', 'views', 'likes', 'ctr']
    #metrics_list_mess - это перечень метрик для мониторинга из таблицы по мессенджеру
    metrics_list_mess = ['message_count']

    #для каждой метрики ленты новостей проверяем наличие аномалий
    #если аномалия есть, то формируем алерт и отправляем в телеграм-бот
    for metric in metrics_list:
        print(metric)
        df2 = df[['ts', 'date', 'hm', metric]].copy()
        is_alert, df3 = check_anomaly(df2, metric)

        if is_alert == 1:
            msg = '''Метрика {metric}:\n текущее значение {current_val:.2f}\nотклонение от предыдущего значения {last_val_diff:.2f}'''.format(
                metric=metric, current_val=df3[metric].iloc[-1],
                last_val_diff=abs(1 - (df3[metric].iloc[-1] / df3[metric].iloc[-2])))

            sns.set(rc={'figure.figsize': (16, 10)})
            plt.tight_layout()

            ax = sns.lineplot(x=df3['ts'], y=df3[metric], label='metric')
            ax = sns.lineplot(x=df3['ts'], y=df3['up'], label='up')
            ax = sns.lineplot(x=df3['ts'], y=df3['low'], label='low')

            for ind, label in enumerate(ax.get_xticklabels()):
                if ind % 2 == 0:
                    label.set_visible(True)
                else:
                    label.set_visible(False)

            ax.set(xlabel='time')
            ax.set(ylabel=metric)

            ax.set_title(metric)
            ax.set(ylim=(0, None))

            plot_object = io.BytesIO()
            ax.figure.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = '{0}.png'.format(metric)
            plt.close()

            bot.sendMessage(chat_id=chat_id, text=msg)
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    #для каждой метрики мессенджера проверяем наличие аномалий
    #если аномалия есть, то формируем алерт и отправляем в телеграм-бот
    for metric in metrics_list_mess:
        print(metric)
        df2 = df_mess[['ts', 'date', 'hm', metric]].copy()
        is_alert, df3 = check_anomaly(df2, metric)

        if is_alert == 1:
            msg = '''Метрика {metric}:\n текущее значение {current_val:.2f}\nотклонение от предыдущего значения {last_val_diff:.2f}'''.format(
                metric=metric, current_val=df3[metric].iloc[-1],
                last_val_diff=abs(1 - (df3[metric].iloc[-1] / df3[metric].iloc[-2])))

            sns.set(rc={'figure.figsize': (16, 10)})
            plt.tight_layout()

            ax = sns.lineplot(x=df3['ts'], y=df3[metric], label='metric')
            ax = sns.lineplot(x=df3['ts'], y=df3['up'], label='up')
            ax = sns.lineplot(x=df3['ts'], y=df3['low'], label='low')

            for ind, label in enumerate(ax.get_xticklabels()):
                if ind % 2 == 0:
                    label.set_visible(True)
                else:
                    label.set_visible(False)

            ax.set(xlabel='time')
            ax.set(ylabel=metric)

            ax.set_title(metric)
            ax.set(ylim=(0, None))

            plot_object = io.BytesIO()
            ax.figure.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = '{0}.png'.format(metric)
            plt.close()

            bot.sendMessage(chat_id=chat_id, text=msg)
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    return


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_arogn_alert():
    @task()
    def make_run_alerts():
        run_alerts()

    make_run_alerts()


dag_arogn_alert = dag_arogn_alert()