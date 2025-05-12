from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import mysql.connector
import psycopg2

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'email': ['ehsanullahdev@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

def extract_from_mysql():
    conn = mysql.connector.connect(
        host='host.docker.internal',  # for Docker to access host DB
        user='root',
        password='rootpassword',
        database='employees'
    )
    cursor = conn.cursor()
    cursor.execute("SELECT emp_no, first_name, last_name, gender FROM employees")
    rows = cursor.fetchall()
    conn.close()
    return rows

def load_to_postgres(ti):
    rows = ti.xcom_pull(task_ids='extract_mysql')
    conn = psycopg2.connect(
        host='postgres_etl',
        dbname='mydb',
        user='user',
        password='userpassword'
    )
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS employees_etl (
            emp_no INT PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender CHAR(1)
        );
    """)
    for row in rows:
        cur.execute("""
            INSERT INTO employees_etl (emp_no, first_name, last_name, gender)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (emp_no) DO NOTHING;
        """, row)
    conn.commit()
    conn.close()



with DAG(
    dag_id='mysql_to_postgres_etl',
    default_args=default_args,
    description='ETL from MySQL to PostgreSQL',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['ETL'],
) as dag:

    extract = PythonOperator(
        task_id='extract_mysql',
        python_callable=extract_from_mysql,
    )

    load = PythonOperator(
        task_id='load_postgres',
        python_callable=load_to_postgres,
    )

    extract >> load





