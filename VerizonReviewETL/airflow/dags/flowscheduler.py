from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


default_args = {
    'owner': "Charlotte",
    'start_date': datetime(2021, 8, 7),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['cxia62@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
}

## defining the DAG using context manager
with DAG(
        'verizon-reviewscrap-activity',
        description='DAG for verizon review scraping ETL pipeline',
        default_args=default_args,
        schedule_interval='@hourly',
) as dag:
    t1 = BashOperator(
        task_id='review-info-webscrap-activity',
        bash_command='python3 D:/Verizon/WebScrapETL/VerizonReviewETL/webscrap.py'
    )

    t2 = BashOperator(
        task_id='pyspark-wordcount-activity',
        bash_command='python3 D:/Verizon/WebScrapETL/VerizonReviewETL/wordcount.py'
    )

    t1 >> t2  ## define dependency