from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

from pipelines.concat_task import concat_data_task
from pipelines.scrape_task import scrape_task


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'email': ['szczepanik.antoni@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('flats_data',
          default_args=default_args,
          schedule_interval=None)

concat = PythonOperator(
    task_id='concat_data',
    python_callable=concat_data_task,
    dag=dag)

scrape = PythonOperator(
    task_id='scrape',
    python_callable=scrape_task,
    dag=dag)


scrape >> concat
