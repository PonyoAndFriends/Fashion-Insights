from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='test_kubernetes_pod_operator',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 12, 25),
    catchup=False,
) as dag:

    kubernetes_operator_task = KubernetesPodOperator(
        task_id='test_kubernetes_pod_operator',
        namespace='airflow',
        image='busybox',
        cmds=["sh", "-c"],
        arguments=["echo 'KubernetesPodOperator Task started'; sleep 180; echo 'KubernetesPodOperator Task completed'"],
        name="test-kubernetes-pod",
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
    )
