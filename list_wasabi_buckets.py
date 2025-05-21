from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import boto3

# Wasabi S3-compatible settings
WASABI_ENDPOINT = 'https://eu-central-2.wasabisys.com'
ACCESS_KEY = '1466DTMUZW200HWI0SK6'
SECRET_KEY = '3CTfHwnYHumN9f6PbeIh3UJkCRoGwi4ivXhGH2He'

# Function to list buckets
def list_wasabi_buckets():
    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        endpoint_url=WASABI_ENDPOINT
    )

    response = s3.list_buckets()
    buckets = [bucket['Name'] for bucket in response.get('Buckets', [])]
    print("Buckets available in Wasabi S3:")
    for bucket in buckets:
        print(f" - {bucket}")
        
def list_objects_in_prefix():
    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        endpoint_url=WASABI_ENDPOINT
    )

    bucket_name = 'mp-temp'
    prefix = 'backups/mp-sandbox1/uploaded'  # Important: trailing slash

    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    contents = response.get('Contents', [])
    if not contents:
        print(f"No objects found under s3://{bucket_name}/{prefix}")
        return

    print(f"Objects under s3://{bucket_name}/{prefix}:")
    for obj in contents:
        print(f" - {obj['Key']} (Size: {obj['Size']} bytes)")


# Airflow DAG definition
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}

dag = DAG(
    'list_wasabi_buckets',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

# list_buckets_task = PythonOperator(
#     task_id='list_wasabi_buckets_task',
#     python_callable=list_wasabi_buckets,
#     dag=dag
# )

# list_buckets_task


list_objects_task = PythonOperator(
    task_id='list_objects_in_prefix',
    python_callable=list_objects_in_prefix,
    dag=dag
)

list_objects_task
