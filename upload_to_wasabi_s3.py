from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import boto3
import os
import subprocess
from airflow.utils.dates import days_ago

# Wasabi S3-compatible settings
WASABI_ENDPOINT = 'https://eu-central-2.wasabisys.com'
ACCESS_KEY = '1466DTMUZW200HWI0SK6'
SECRET_KEY = '3CTfHwnYHumN9f6PbeIh3UJkCRoGwi4ivXhGH2He'

def pg_dump_backup():
    dump_file = '/tmp/postgres_backup1.sql'  
    # cmd = [
    #     'pg_dump',
    #     '-h', 'postgres_etl',
    #     '-U', 'user',
    #     '-d', 'mydb',
    #     '-f', dump_file
    # ]
    cmd = [
        'pg_dump',
        '-h', 'vmd167397.contaboserver.net',
        '-p', '30012',
        '-U', 'airflow',
        '-d', 'airflow',
        '-f', dump_file
    ]
    # env = {'PGPASSWORD': 'userpassword'}
    env = {'PGPASSWORD': 'Metapolaris123A'}
    subprocess.run(cmd, check=True, env={**env, **dict(**env)})
    # env = os.environ.copy()
    # env['PGPASSWORD'] = 'Metapolaris123A'
    # subprocess.run(cmd, check=True, env=env)
    print(f"Backup saved to {dump_file}")

# Upload function
def upload_to_wasabi():
    # file_path = '/opt/airflow/data/dummyfile.txt'
    file_path = '/tmp/postgres_backup1.sql'  # Path to the file to upload
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file {file_path} does not exist.")
    
    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        endpoint_url=WASABI_ENDPOINT
    )

    bucket_name = 'mp-temp'
    object_key = 'backups/mp-sandbox1/uploaded/backup_db1.sql'  # ✅ specific path

    # Ensure the bucket exists (optional)
    try:
        s3.head_bucket(Bucket=bucket_name)
    except s3.exceptions.ClientError:
        s3.create_bucket(Bucket=bucket_name)

    # Upload the file
    s3.upload_file(file_path, bucket_name, object_key)
    print(f"Uploaded {file_path} to s3://{bucket_name}/{object_key}")

def backup_and_upload():
    pg_dump_backup()
    upload_to_wasabi()




# # Airflow DAG definition
# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2023, 1, 1),
#     'retries': 1
# }

# dag = DAG(
#     'upload_to_wasabi_s3',
#     default_args=default_args,
#     schedule_interval=None,
#     catchup=False,
#     start_date=datetime(2023, 1, 1),
# )

# upload_task = PythonOperator(
#     task_id='upload_file_to_wasabi',
#     python_callable=upload_to_wasabi,
#     dag=dag
# )

# backup = PythonOperator(
#         task_id='backup_postgres',
#         python_callable=pg_dump_backup,
#     )


# backup >> upload_task
# -------------------------------------------------------------------
# DAG definition
# -------------------------------------------------------------------
with DAG(
    dag_id="upload_to_wasabi_s3",
    start_date=days_ago(1),          # any past date works
    schedule_interval='@daily'

) as dag:

    backup_and_upload = PythonOperator(
        task_id="backup_and_upload",
        python_callable=backup_and_upload,
    )
    # backup = PythonOperator(
    #     task_id="backup_postgres",
    #     python_callable=pg_dump_backup,
    # )

    # upload = PythonOperator(
    #     task_id="upload_backup_to_wasabi",
    #     python_callable=upload_to_wasabi,
    # )

    # backup >> upload