from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from kubernetes import client, config
from airflow.models import Variable


class SparkSubmitOperator(BaseOperator):
    """
    Spark job을 airflow에서 간단히 제출하기 위한 Custom Operator

    Args:
        name (str): 스파크 어플리케이션 이름, 같은 이름이 있을 시 지우고 시작됨.
        namespace (str): K8S 상의 네임스페이스 명, 해당 네임 스페이스 내에 spark job이 실행 될 pod가 실행됨.
        image (str): 스파크 잡을 실행할 driver와 executor를 위한 이미지 깃헙 액션으로 자동 업데이트 됨.
        main_application_file_name (str): 본인이 실행할 pyspark 코드 파일 이름, 레포의 spark_job 폴더 내에 작성되어야함.
        spark_version (str): 스파크 버전 - 3.5.4를 권장
        driver_cores (int): 드라이버의 cpu 코어 수 
        driver_memory (str): 드라이버의 메모리 
        executor_cores (int): executor의 코어 수
        executor_memory (str): executor의 메모리 수
        executor_instances (int): executor의 수
        spark_conf (dict): 추가로 적용하고 싶은 설정을 담는 dictionary
        dependencies (list): Jar 파일로 추가할 종속성
    """

    @apply_defaults
    def __init__(
        self,
        name: str,  # 필수값
        namespace: str = "default",
        image: str = "coffeeisnan/spark-job:latest",
        main_application_file_name: str = "pyspark_s3_example.py",
        spark_version: str = "3.5.4",
        driver_cores: int = 1,
        driver_memory: str = "1g",
        executor_cores: int = 1,
        executor_memory: str = "1g",
        executor_instances: int = 2,
        spark_conf: dict = None,
        dependencies: list = None,
        *args,
        **kwargs,
    ):
        super(SparkSubmitOperator, self).__init__(*args, **kwargs)
        self.name = name
        self.namespace = namespace
        self.image = image
        self.main_application_file = f"local:///opt/spark/jobs/{main_application_file_name}"
        self.spark_version = spark_version
        self.driver_cores = driver_cores
        self.driver_memory = driver_memory
        self.executor_cores = executor_cores
        self.executor_memory = executor_memory
        self.executor_instances = executor_instances
        self.spark_conf = spark_conf or {}
        self.dependencies = dependencies or []

    def execute(self, context):
        config.load_incluster_config()
        api_instance = client.CustomObjectsApi()

        try:
            api_instance.delete_namespaced_custom_object(
                group="sparkoperator.k8s.io",
                version="v1beta2",
                namespace=self.namespace,
                plural="sparkapplications",
                name=self.name,
            )
        except client.exceptions.ApiException as e:
            if e.status != 404:
                raise

        default_spark_conf = {
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.access.key": Variable.get("aws_access_key_id"),
            "spark.hadoop.fs.s3a.secret.key": Variable.get("aws_secret_access_key"),
            "spark.hadoop.fs.s3a.endpoint": "s3.ap-northeast-2.amazonaws.com",
            "spark.sql.parquet.compression.codec": "snappy",
        }
        default_spark_conf.update(self.spark_conf)

        spark_application = {
            "apiVersion": "sparkoperator.k8s.io/v1beta2",
            "kind": "SparkApplication",
            "metadata": {"name": self.name, "namespace": self.namespace},
            "spec": {
                "type": "Python",
                "mode": "cluster",
                "image": self.image,
                "imagePullPolicy": "Always",
                "mainApplicationFile": self.main_application_file,
                "sparkVersion": self.spark_version,
                "restartPolicy": {"type": "Never"},
                "driver": {
                    "cores": self.driver_cores,
                    "memory": self.driver_memory,
                    "serviceAccount": "spark-driver-sa",
                },
                "executor": {
                    "cores": self.executor_cores,
                    "memory": self.executor_memory,
                    "instances": self.executor_instances,
                },
                "sparkConf": default_spark_conf,
                "deps": {"jars": self.dependencies},
            },
        }

        api_instance.create_namespaced_custom_object(
            group="sparkoperator.k8s.io",
            version="v1beta2",
            namespace=self.namespace,
            plural="sparkapplications",
            body=spark_application,
        )
