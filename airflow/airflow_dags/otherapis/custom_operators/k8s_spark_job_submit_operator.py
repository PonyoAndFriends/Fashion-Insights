from airflow.models import BaseOperator
from kubernetes import client, config
from custom_modules.spark_dependencies import *
import time


class SparkApplicationOperator(BaseOperator):
    """
    Airflow에서 같은 EKS 클러스터 내의 EKS에게 spark job을 제출하기 위한 커스텀 오퍼레이터

    필수 파라미터
        - name: spark application의 이름
        - main_application_file: 스파크 실행 파일의 이미지 내 경로, /opt/spark/jobs/ 가 우리 레포 기준 spark/spark_job 이 됨

    선택 파라미터
        - application_args: 전달하고 싶은 인자 (데이터를 읽어들일 버킷의 경로 등을 전달 가능), 리스트로 여러 개 전달.
        - driver_config: 스파크 driver의 스펙을 설정 가능.
        - executor_config: 스파크 executor의 스펙을 설정 가능.
    """

    def __init__(
        self,
        name,
        main_application_file,  # 레포 기준 spark_job 아래부터 경로를 작성
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
        self.main_application_file = "/opt/spark/jobs/" + main_application_file
        self.spark_version = spark_version
        self.driver_config = driver_config
        self.executor_config = executor_config
        self.deps = deps
        self.spark_conf = spark_conf

    def wait_for_completion(self, api_instance):
        """Wait for the SparkApplication to complete and fetch the status."""
        while True:
            response = api_instance.get_namespaced_custom_object(
                group="sparkoperator.k8s.io",
                version="v1beta2",
                namespace=self.namespace,
                plural="sparkapplications",
                name=self.name,
            )
            state = (
                response.get("status", {})
                .get("applicationState", {})
                .get("state", "UNKNOWN")
            )
            self.log.info(f"SparkApplication {self.name} current state: {state}")

            if state in ["COMPLETED", "FAILED", "ERROR"]:
                self.log.info(
                    f"SparkApplication {self.name} finished with state: {state}"
                )
                return state
            time.sleep(5)

    def fetch_driver_logs(self):
        """Fetch logs from the Driver Pod."""
        core_v1_api = client.CoreV1Api()
        driver_pod_name = f"spark-{self.name}-driver"
        try:
            logs = core_v1_api.read_namespaced_pod_log(
                name=driver_pod_name, namespace=self.namespace
            )
            self.log.info(f"Driver Pod Logs for {driver_pod_name}:\n{logs}")
        except Exception as e:
            self.log.error(f"Failed to fetch driver logs: {e}")

    def execute(self, context):
        self.log.info("Loading Kubernetes configuration.")
        config.load_incluster_config()
        api_instance = client.CustomObjectsApi()

        # 기존 SparkApplication 삭제
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

        # SparkApplication 생성
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
                "cleanPodPolicy": "Always",
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

        # 상태 확인 및 Driver 로그 가져오기
        state = self.wait_for_completion(api_instance)
        self.fetch_driver_logs()

        # 작업 실패 시 예외 발생
        if state not in ["COMPLETED"]:
            raise Exception(f"SparkApplication {self.name} failed with state: {state}")
