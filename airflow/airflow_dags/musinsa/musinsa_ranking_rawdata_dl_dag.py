from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.dummy import DummyOperator

from airflow.utils.dates import days_ago
from datetime import timedelta
from kubernetes.client import models as k8s

# DAG 기본 설정
default_args = {
    'owner': 'ehdgml7755@cu.ac.kr',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 24,
    'retry_delay': timedelta(minutes=30),
}

SEXUAL_DYNAMIC_PARAMS = [{
    "SEXUAL" : "F"
}, {
    "SEXUAL" : "M"
}, {
    "SEXUAL" : "A"
}
]

AGE_BAND_DYNAMIC_PARAMS = [{
    "AGE_BAND" : "AGE_BAND_ALL"
}, {
    "AGE_BAND" : "AGE_BAND_MINOR"
}, {
    "AGE_BAND" : "AGE_BAND_20"
}, {
    "AGE_BAND" : "AGE_BAND_25"
}, {
    "AGE_BAND" : "AGE_BAND_30"
}, {
    "AGE_BAND" : "AGE_BAND_35"
}, {
    "AGE_BAND" : "AGE_BAND_40"
}
]

# DAG 정의
with DAG(
    dag_id='Musinsa_Ranking_RawData_EL_DAG',
    default_args=default_args,
    description='musinsa ranking raw data extraction and loading to s3',
    schedule_interval='0 0 * * *',
    start_date=days_ago(1),
    catchup=False,
    tags=['musinsa', 'ranking_rawdata', 'Extract', 'Load', 'S3', 'k8s']
) as dag:

    # Start task
    start = DummyOperator(
                task_id="start"
            )
    
    for i, sexual_dct in enumerate(SEXUAL_DYNAMIC_PARAMS):
        sexual_task = DummyOperator(
                        task_id=f'{sexual_dct["SEXUAL"]}_task'
        )
        
        start >> sexual_task
        
        for j, age_band_dct in enumerate(AGE_BAND_DYNAMIC_PARAMS):
            ageband_task = KubernetesPodOperator(
                                task_id=f'{sexual_dct["SEXUAL"]}_{age_band_dct["AGE_BAND"]}_task',
                                name=f'{sexual_dct["SEXUAL"]}_{age_band_dct["AGE_BAND"]}_task',
                                namespace='airflow',
                                image='coffeeisnan/project4-custom:latest',
                                cmds=['python', './pythonscript/musinsa_ranking_rawdata_el.py'],
                                arguments=[sexual_dct["SEXUAL"], age_band_dct["AGE_BAND"]],
                                # is_delete_operator_pod=True,
                                get_logs=True,
            )
            
            sexual_task >> ageband_task
