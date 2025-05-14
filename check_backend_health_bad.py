
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'email': ['ehsanullahdev@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

def check_backend():
    url = "http://host.docker.internal:3001/ok1"  # Replace with appropriate service name or host
    try:
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(f"Unexpected status code: {response.status_code}")
        print("✅ Backend is UP")
    except Exception as e:
        # This raises an error to make the task fail and trigger Airflow's failure alert
        raise RuntimeError(f"❌ Backend health check failed: {str(e)}")

with DAG(
    dag_id='check_backend_health_bad',
    default_args=default_args,
    description='Checks backend health and alerts via email on failure',
    schedule_interval='*/10 * * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['healthcheck', 'alerts'],
) as dag:

    check = PythonOperator(
        task_id='check_backend_health_bad',
        python_callable=check_backend,
    )
