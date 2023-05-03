import datetime as dt
import os
import sys
import logging

from airflow.models import DAG
from airflow.operators.python import PythonOperator

path = os.path.expanduser('~/airflow_hw')
logging.info('Путь к папке с проектом: ' + path)
# Добавим путь к коду проекта в переменную окружения, чтобы он был доступен python-процессу
os.environ['PROJECT_PATH'] = path
logging.info('Путь к папке с проектом в ОС: ' + os.environ['PROJECT_PATH'])
# Добавим путь к коду проекта в $PATH, чтобы импортировать функции
sys.path.insert(0, path)
logging.info('Пути поиска модулей: ' + sys.path[0])

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2022, 6, 11),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False,
}

with DAG(
        dag_id='car_price_prediction',
        schedule_interval="00 15 * * *",
        default_args=args,
) as dag:
    from modules.pipeline import pipeline
    from modules.predict import predict

    pipeline = PythonOperator(
        task_id='pipeline',
        python_callable=pipeline,
    )
    predict = PythonOperator(
        task_id='predict',
        python_callable=predict,
    )
    pipeline >> predict
