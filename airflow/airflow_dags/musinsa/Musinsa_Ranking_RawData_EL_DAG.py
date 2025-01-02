from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.dummy import DummyOperator

from airflow.utils.dates import days_ago
from datetime import timedelta
from kubernetes.client import models as k8s

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

Sexual_Dynamic_Params = [{
    "SEXUAL" : "F"
}, {
    "SEXUAL" : "M"
}, {
    "SEXUAL" : "A"
}
]

# DAG 정의
with DAG(
    dag_id='Musinsa_Ranking_RawData_EL_DAG',
    default_args=default_args,
    description='Example DAG using dynamic Python tasks in KubernetesExecutor',
    schedule_interval=None,  # 수동 실행
    start_date=days_ago(1),
    catchup=False,
    tags=['example', 'k8s', 'dynamic-python'],
) as dag:

    # Start task
    start = DummyOperator(
        task_id="start")

    for i, sexual_dct in enumerate(Sexual_Dynamic_Params):
        sexual_task = KubernetesPodOperator(
            task_id=f'{sexual_dct["SEXUAL"]}_task',
            name=f'{sexual_dct["SEXUAL"]}_task',
            namespace='airflow',
            image='ehdgml7755/project4-custom:latest',
            cmds=['python', './pythonscript/Musinsa_Ranking_RawData_EL.py'],  # Execute the script from /scripts
            arguments=[sexual_dct["SEXUAL"]],  # Pass task_number as an argument to the script
            is_delete_operator_pod=True,  # Do not delete pod after completion
            get_logs=True,
        )
        start >> sexual_task