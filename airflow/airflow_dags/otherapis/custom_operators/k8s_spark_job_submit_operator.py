from airflow.models import BaseOperator
from kubernetes import client, config
from custom_modules.spark_dependencies import *


class SparkApplicationOperator(BaseOperator):
    """
    Airflow에서 같은 EKS 클러스터 내의 EKS에게 spark job을 제출하기 위한 커스텀 오퍼레이터

    필수 파라미터
        - name: spark application의 이름
        - main_application_file: 스파크 실행 파일의 이미지 내 경로, /opt/spark/jobs/ 가 우리 레포 기준 spark/spark_job 이 됨

    선택 파라미터
        - application_args: 전달하고 싶은 인자 (데이터를 읽어들일 버킷의 경로 등을 전달 가능), 리스트로 여러개 전달.
        - driver_config: 스파크 driver의 스펙을 설정 가능.
        - executor_config: 스파크 executor의 스펙을 설정 가능
    """

    def __init__(
        self,
        name,
        main_application_file,
        application_args=None,
        driver_config=SPARK_DRIVER_DEFAULT_CONFIG,
        executor_config=SPARK_EXECUTOR_DEFAULT_CONFIG,
        spark_version="3.5.4",
        image="coffeeisnan/spark-job:latest",
        namespace="defaults",
        deps=SPARK_DEFULAT_DEPS,
        spark_conf=SPARK_DEFAULT_CONF,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.name = name
        self.namespace = namespace
        self.application_args = application_args
        self.image = image
        self.main_application_file = main_application_file
        self.spark_version = spark_version
        self.driver_config = driver_config
        self.executor_config = executor_config
        self.deps = deps
        self.spark_conf = spark_conf

    def execute(self, context):
        self.log.info("Loading Kubernetes configuration.")
        config.load_incluster_config()

        api_instance = client.CustomObjectsApi()

        try:
            self.log.info(f"Deleting existing SparkApplication: {self.name}")
            api_instance.delete_namespaced_custom_object(
                group="sparkoperator.k8s.io",
                version="v1beta2",
                namespace=self.namespace,
                plural="sparkapplications",
                name=self.name,
            )
        except client.exceptions.ApiException as e:
            if e.status != 404:
                self.log.error(f"Failed to delete existing SparkApplication: {e}")
                raise
            self.log.info(
                f"No existing SparkApplication named {self.name} found. Proceeding with creation."
            )

        spark_application = {
            "apiVersion": "sparkoperator.k8s.io/v1beta2",
            "kind": "SparkApplication",
            "metadata": {
                "name": self.name,
                "namespace": self.namespace,
            },
            "spec": {
                "type": "Python",
                "mode": "cluster",
                "image": self.image,
                "imagePullPolicy": "Always",
                "mainApplicationFile": self.main_application_file,
                "sparkVersion": self.spark_version,
                "restartPolicy": {"type": "Never"},
                "driver": self.driver_config,
                "executor": self.executor_config,
                "deps": self.deps,
                "sparkConf": self.spark_conf,
                "arguments": self.application_args,
            },
        }

        self.log.info(f"Creating SparkApplication: {self.name}")
        api_instance.create_namespaced_custom_object(
            group="sparkoperator.k8s.io",
            version="v1beta2",
            namespace=self.namespace,
            plural="sparkapplications",
            body=spark_application,
        )
        self.log.info(f"SparkApplication {self.name} created successfully.")
