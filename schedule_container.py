from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 4, 23),
}

# Define the DAG
dag = DAG(
    'docker_container_dag',
    default_args=default_args,
    description='A DAG to run a dummy Docker container',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['docker', 'example'],
)

# Define a function for our Python operator
def print_hello():
    return 'Hello from Airflow!'

# Create a start task
start_task = DummyOperator(
    task_id='start_pipeline',
    dag=dag,
)

# Create a Python task
hello_task = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag,
)

# Create a task to run a Docker container
# Using the official Python image as an example
docker_task = DockerOperator(
    task_id='docker_command',
    image='python:3.9-slim',
    command='python -c "import time; print(\'Container is running!\'); time.sleep(10); print(\'Container work complete!\')"',
    container_name='airflow_docker_demo',
    docker_url='unix://var/run/docker.sock',  # Use this for local Docker daemon
    network_mode='bridge',
    auto_remove=True,
    # Fix for bytes serialization error
    do_xcom_push=False,  # Disable XCom push to avoid serialization issues
    dag=dag,
)

# Create an end task
end_task = DummyOperator(
    task_id='end_pipeline',
    dag=dag,
)

# Define the task dependencies
start_task >> hello_task >> docker_task >> end_task