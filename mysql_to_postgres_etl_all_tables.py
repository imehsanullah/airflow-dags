from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import mysql.connector
import psycopg2
import logging

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Configuration
MYSQL_CONFIG = {
    'host': 'host.docker.internal',  # for Docker to access host DB
    'user': 'root',
    'password': 'rootpassword',
    'database': 'employees'
}

POSTGRES_CONFIG = {
    'host': 'postgres_etl',
    'dbname': 'mydb',
    'user': 'user',
    'password': 'userpassword'
}

# Define table extraction and loading functions
def extract_table(table_name, columns="*"):
    """Generic function to extract data from a MySQL table"""
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()
    
    query = f"SELECT {columns} FROM {table_name}"
    logging.info(f"Executing query: {query}")
    
    cursor.execute(query)
    rows = cursor.fetchall()
    
    # Get column names
    column_names = [i[0] for i in cursor.description]
    
    # Filter out date-related columns
    date_column_indices = [i for i, col in enumerate(column_names) if 
                           'date' in col.lower() or 
                           col.lower() == 'from_date' or 
                           col.lower() == 'to_date']
    
    # Filter columns and data
    filtered_column_names = [col for i, col in enumerate(column_names) if i not in date_column_indices]
    filtered_rows = []
    for row in rows:
        filtered_row = [val for i, val in enumerate(row) if i not in date_column_indices]
        filtered_rows.append(filtered_row)
    
    conn.close()
    return {"rows": filtered_rows, "columns": filtered_column_names}

def create_postgres_table(conn, table_name, schema):
    """Create table in PostgreSQL if it doesn't exist"""
    cursor = conn.cursor()
    cursor.execute(schema)
    conn.commit()

def load_to_postgres(ti, table_name, postgres_schema):
    """Generic function to load data into PostgreSQL"""
    data = ti.xcom_pull(task_ids=f'extract_{table_name}')
    if not data or 'rows' not in data or not data['rows']:
        logging.warning(f"No data found for {table_name}")
        return
    
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    
    # Create table if it doesn't exist (using modified schema)
    create_postgres_table(conn, table_name, postgres_schema)
    
    # Insert data
    cursor = conn.cursor()
    columns = data['columns']
    columns_str = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    
    for row in data['rows']:
        query = f"""
            INSERT INTO {table_name} ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT DO NOTHING;
        """
        cursor.execute(query, row)
    
    conn.commit()
    conn.close()
    logging.info(f"Loaded {len(data['rows'])} rows into {table_name}")

# Define specific extract functions for each table
def extract_departments():
    return extract_table('departments')

def extract_employees():
    # Explicitly exclude date columns
    return extract_table('employees', columns="emp_no, first_name, last_name, gender")

def extract_dept_emp():
    # Explicitly exclude date columns
    return extract_table('dept_emp', columns="emp_no, dept_no")

def extract_dept_manager():
    # Explicitly exclude date columns
    return extract_table('dept_manager', columns="emp_no, dept_no")

def extract_salaries():
    # Explicitly exclude date columns
    return extract_table('salaries', columns="emp_no, salary")

def extract_titles():
    # Explicitly exclude date columns
    return extract_table('titles', columns="emp_no, title")

def extract_dept_emp_latest_date():
    # Explicitly exclude date columns
    return extract_table('dept_emp_latest_date', columns="emp_no")

def extract_current_dept_emp():
    # Explicitly exclude date columns
    return extract_table('current_dept_emp', columns="emp_no, dept_no")

# Define load functions with modified schemas (no date columns)
def load_departments(ti):
    schema = """
    CREATE TABLE IF NOT EXISTS departments (
        dept_no CHAR(4) PRIMARY KEY,
        dept_name VARCHAR(40) UNIQUE NOT NULL
    );
    """
    load_to_postgres(ti, 'departments', schema)

def load_employees(ti):
    schema = """
    CREATE TABLE IF NOT EXISTS employees (
        emp_no INT PRIMARY KEY,
        first_name VARCHAR(14) NOT NULL,
        last_name VARCHAR(16) NOT NULL,
        gender CHAR(1) NOT NULL
    );
    """
    load_to_postgres(ti, 'employees', schema)

def load_dept_emp(ti):
    schema = """
    CREATE TABLE IF NOT EXISTS dept_emp (
        emp_no INT NOT NULL,
        dept_no CHAR(4) NOT NULL,
        PRIMARY KEY (emp_no, dept_no)
    );
    """
    load_to_postgres(ti, 'dept_emp', schema)

def load_dept_manager(ti):
    schema = """
    CREATE TABLE IF NOT EXISTS dept_manager (
        emp_no INT NOT NULL,
        dept_no CHAR(4) NOT NULL,
        PRIMARY KEY (emp_no, dept_no)
    );
    """
    load_to_postgres(ti, 'dept_manager', schema)

def load_salaries(ti):
    schema = """
    CREATE TABLE IF NOT EXISTS salaries (
        emp_no INT NOT NULL,
        salary INT NOT NULL,
        PRIMARY KEY (emp_no)
    );
    """
    load_to_postgres(ti, 'salaries', schema)

def load_titles(ti):
    schema = """
    CREATE TABLE IF NOT EXISTS titles (
        emp_no INT NOT NULL,
        title VARCHAR(50) NOT NULL,
        PRIMARY KEY (emp_no, title)
    );
    """
    load_to_postgres(ti, 'titles', schema)

def load_dept_emp_latest_date(ti):
    schema = """
    CREATE TABLE IF NOT EXISTS dept_emp_latest_date (
        emp_no INT PRIMARY KEY
    );
    """
    load_to_postgres(ti, 'dept_emp_latest_date', schema)

def load_current_dept_emp(ti):
    schema = """
    CREATE TABLE IF NOT EXISTS current_dept_emp (
        emp_no INT NOT NULL,
        dept_no CHAR(4) NOT NULL,
        PRIMARY KEY (emp_no, dept_no)
    );
    """
    load_to_postgres(ti, 'current_dept_emp', schema)

# Create DAG
with DAG(
    dag_id='mysql_to_postgres_etl_all_tables',
    default_args=default_args,
    description='ETL from MySQL to PostgreSQL for all employee tables (excluding date columns)',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['ETL'],
) as dag:
    
    # Extract tasks
    extract_departments_task = PythonOperator(
        task_id='extract_departments',
        python_callable=extract_departments,
    )
    
    extract_employees_task = PythonOperator(
        task_id='extract_employees',
        python_callable=extract_employees,
    )
    
    extract_dept_emp_task = PythonOperator(
        task_id='extract_dept_emp',
        python_callable=extract_dept_emp,
    )
    
    extract_dept_manager_task = PythonOperator(
        task_id='extract_dept_manager',
        python_callable=extract_dept_manager,
    )
    
    extract_salaries_task = PythonOperator(
        task_id='extract_salaries',
        python_callable=extract_salaries,
    )
    
    extract_titles_task = PythonOperator(
        task_id='extract_titles',
        python_callable=extract_titles,
    )
    
    extract_dept_emp_latest_date_task = PythonOperator(
        task_id='extract_dept_emp_latest_date',
        python_callable=extract_dept_emp_latest_date,
    )
    
    extract_current_dept_emp_task = PythonOperator(
        task_id='extract_current_dept_emp',
        python_callable=extract_current_dept_emp,
    )
    
    # Load tasks
    load_departments_task = PythonOperator(
        task_id='load_departments',
        python_callable=load_departments,
    )
    
    load_employees_task = PythonOperator(
        task_id='load_employees',
        python_callable=load_employees,
    )
    
    load_dept_emp_task = PythonOperator(
        task_id='load_dept_emp',
        python_callable=load_dept_emp,
    )
    
    load_dept_manager_task = PythonOperator(
        task_id='load_dept_manager',
        python_callable=load_dept_manager,
    )
    
    load_salaries_task = PythonOperator(
        task_id='load_salaries',
        python_callable=load_salaries,
    )
    
    load_titles_task = PythonOperator(
        task_id='load_titles',
        python_callable=load_titles,
    )
    
    load_dept_emp_latest_date_task = PythonOperator(
        task_id='load_dept_emp_latest_date',
        python_callable=load_dept_emp_latest_date,
    )
    
    load_current_dept_emp_task = PythonOperator(
        task_id='load_current_dept_emp',
        python_callable=load_current_dept_emp,
    )
    
    # Connect extract tasks directly to their corresponding load tasks
    extract_departments_task >> load_departments_task
    extract_employees_task >> load_employees_task
    extract_dept_emp_task >> load_dept_emp_task
    extract_dept_manager_task >> load_dept_manager_task
    extract_salaries_task >> load_salaries_task
    extract_titles_task >> load_titles_task
    extract_dept_emp_latest_date_task >> load_dept_emp_latest_date_task
    extract_current_dept_emp_task >> load_current_dept_emp_task