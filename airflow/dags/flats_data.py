from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from common import DATA_TYPES
from pipelines.scrape_task import scrape_task
from pipelines.concat_task import concat_data_task
from pipelines.cleaning_task import cleaning_task
from pipelines.feature_engineering_task import feature_engineering_task
from pipelines.apply_task import apply_task
from pipelines.update_website_data import update_website_data_task

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['szczepanik.antoni@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG('flats_data',
          default_args=default_args,
          schedule_interval='15 0 * * *')

for data_type in DATA_TYPES:

    scrape = PythonOperator(
        task_id=f'scrape_{data_type}',
        python_callable=scrape_task,
        op_kwargs={'data_type': data_type},
        dag=dag)

    concat = PythonOperator(
        task_id=f'concat_{data_type}',
        python_callable=concat_data_task,
        op_kwargs={'data_type': data_type},
        dag=dag)

    clean = PythonOperator(
        task_id=f'clean_{data_type}',
        python_callable=cleaning_task,
        op_kwargs={'data_type': data_type},
        dag=dag)

    engineer_features = PythonOperator(
        task_id=f'enginner_features_{data_type}',
        python_callable=feature_engineering_task,
        op_kwargs={'data_type': data_type},
        dag=dag)

    apply_model = PythonOperator(
        task_id=f'apply_model_{data_type}',
        python_callable=apply_task,
        op_kwargs={'data_type': data_type},
        dag=dag)

    scrape >> concat >> clean >> engineer_features >> apply_model

update_website_data = PythonOperator(
    task_id='update_website',
    python_callable=update_website_data_task,
    dag=dag)

update_website_data
