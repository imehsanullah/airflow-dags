from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="run_container_in_kubernetes",
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False,
) as dag:

    docker_equivalent_task = KubernetesPodOperator(
        task_id='run_python_container',
        name='airflow-k8s-demo',
        namespace='mp-airflow',  # match your Airflow namespace
        image='python:3.9-slim',
        cmds=["python", "-c"],
        arguments=[
            "import time; "
            "print('Container is running!'); "
            "time.sleep(10); "
            "print('Container work complete!')"
        ],
        is_delete_operator_pod=True,
        get_logs=True,
    )
