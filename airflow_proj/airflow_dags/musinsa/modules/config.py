from airflow.models import Variable
from datetime import timedelta
import pendulum


class DEFAULT_SPARK:
    DRIVER_CONFIG = {
        "cores": 1,
        "coreLimit": "1200m",
        "memory": "1g",
        "serviceAccount": "spark-driver-sa",
    }

    EXECUTOR_CONFIG = {
        "cores": 1,
        "instances": 2,  # executor의 pod 개수
        "memory": "1g",
    }

    DEPS = {
        "jars": [
            "local:///opt/spark/user-jars/hadoop-aws-3.3.1.jar",
            "local:///opt/spark/user-jars/aws-java-sdk-bundle-1.11.901.jar",
        ],
    }

    SPARK_CONF = {
        "spark.hadoop.fs.s3a.access.key": Variable.get("aws_access_key_id"),
        "spark.hadoop.fs.s3a.secret.key": Variable.get("aws_secret_access_key"),
        "spark.hadoop.fs.s3a.endpoint": "s3.amazonaws.com",
        "spark.kubernetes.driver.deleteOnTermination": "true",
        "spark.kubernetes.executor.deleteOnTermination": "true",
    }


class DEFAULT_DAG:
    DEFAULT_ARGS = {
        "owner": "ehdgml7755@cu.ac.kr",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 24,
        "retry_delay": timedelta(minutes=30),
    }

    LOCAL_TZ = pendulum.timezone("Asia/Seoul")
