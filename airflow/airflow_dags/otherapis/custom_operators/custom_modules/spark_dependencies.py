from airflow.models import Variable

SPARK_DRIVER_DEFAULT_CONFIG = {
    "cores": 1,
    "coreLimit": "1200m",
    "memory": "1g",
    "serviceAccount": "spark-driver-sa",
}

SPARK_EXECUTOR_DEFAULT_CONFIG = {
    "cores": 1,
    "instances": 2,
    "memory": "1g",
}

SPARK_DEFULAT_CONFIG_DICT_FORMAT = {
    "apiVersion": "sparkoperator.k8s.io/v1beta2",
    "kind": "SparkApplication",
    "metadata": {
        "name": None,
        "namespace": None,
    },
    "spec": {
        "type": "Python",
        "mode": "cluster",
        "image": None,
        "imagePullPolicy": "Always",
        "mainApplicationFile": None,
        "sparkVersion": "3.5.4",
        "restartPolicy": {"type": "Never"},
        "driver": None,
        "executor": None,
        "deps": None,
        "sparkConf": None,
    },
}

SPARK_DEFULAT_DEPS = {
    "jars": [
        "local:///opt/spark/user-jars/hadoop-aws-3.3.1.jar",
        "local:///opt/spark/user-jars/aws-java-sdk-bundle-1.11.901.jar",
    ],
}

SPARK_DEFAULT_CONF = {
    "spark.hadoop.fs.s3a.access.key": Variable.get("aws_access_key_id"),
    "spark.hadoop.fs.s3a.secret.key": Variable.get("aws_secret_access_key"),
    "spark.hadoop.fs.s3a.endpoint": "s3.amazonaws.com",
    "spark.kubernetes.driver.deleteOnTermination": "true",
    "spark.kubernetes.executor.deleteOnTermination": "true",
    "spark.eventLog.enabled": "true",
    "spark.eventLog.dir": "s3a://spark-log-bucket-hs/spark-logs/",
    "spark.history.fs.logDirectory": "s3a://spark-log-bucket-hs/spark-logs/",
}

# 웹 UI 띄울 수 있도록 설정하는 법(pod가 꺼지면 접근 불가)
# "sparkConf": {
#     "spark.ui.enabled": "true",
#     "spark.ui.port": "4040",
#     "spark.kubernetes.driver.serviceAnnotations": {
#         "external-dns.alpha.kubernetes.io/hostname": "spark-driver.example.com"
#     }
# }