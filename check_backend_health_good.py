from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests

def check_backend():
    url = "http://host.docker.internal:3001/ok"  # Use internal host name from inside Docker
    try:
        response = requests.get(url)
        if response.status_code == 200:
            print(f"✅ Backend is UP! Status: {response.status_code}")
        else:
            print(f"⚠️ Backend returned unexpected status: {response.status_code}")
    except requests.RequestException as e:
        print(f"❌ Backend check failed: {e}")

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='check_backend_health_good',
    default_args=default_args,
    description='Check dummy backend every 10 minutes',
    schedule_interval='*/10 * * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['healthcheck', 'good'],
) as dag:

    check_backend_task = PythonOperator(
        task_id='ping_backend',
        python_callable=check_backend
    )

