from airflow import DAG
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from main.weather import get_weather,upload_csv_to_gcs


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 6),
    'email': ['sushithks@gmail.com.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}
dag = DAG(
    'twitter_dag',
    default_args=default_args,
    description='Weather DAG !',
    schedule_interval=timedelta(days=1),
)
dag_start = EmptyOperator(
    task_id="Dag Start"
)
report_generation = PythonOperator(
    task_id='Weather report generation',
    python_callable=get_weather,
    dag=dag,
)
upload_file = PythonOperator(
    task_id='Save to GCS',
    python_callable=upload_csv_to_gcs,
    dag=dag,
)
dag_end = EmptyOperator(
    task_id="Dag Finish"
)
dag_start >> report_generation >> upload_file >> dag_end