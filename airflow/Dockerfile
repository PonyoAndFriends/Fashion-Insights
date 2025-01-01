FROM bitnami/spark:3.5.4

# 필요한 패키지 설치
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    krb5-user \
    curl \
    unzip \
    procps \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Ivy 디렉터리와 필요한 라이브러리 설정
RUN mkdir -p /tmp/.ivy2 && \
    pip install boto3

RUN mkdir -p /tmp/spark-uploads/

ENV SPARK_HOME=/opt/bitnami/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Hadoop 관련 종속성 추가
RUN curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar \
    && curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar \
    && mv hadoop-aws-3.3.2.jar aws-java-sdk-bundle-1.11.1026.jar $SPARK_HOME/jars/

COPY ./airflow_dags/spark_job/s3_example.py /opt/spark/jobs/s3_example.py
COPY ./airflow_dags/spark_job/example_job.py /opt/spark/jobs/example_job.py

# Kerberos 설정 파일 복사
COPY krb5.conf /etc/krb5.conf

# 환경 변수 설정
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV HADOOP_CONF_DIR=/etc/hadoop
ENV KRB5_CONFIG=/etc/krb5.conf

# Spark 환경 설정 추가
RUN echo "spark.driver.extraJavaOptions=-Djava.security.krb5.conf=/etc/krb5.conf" >> $SPARK_HOME/conf/spark-defaults.conf \
    && echo "spark.executor.extraJavaOptions=-Djava.security.krb5.conf=/etc/krb5.conf" >> $SPARK_HOME/conf/spark-defaults.conf

# 사용자 권한 복구
USER 1001
