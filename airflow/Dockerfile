FROM bitnami/spark:3.5.4

# Ivy 디렉터리와 필요한 라이브러리 설정
RUN mkdir -p /tmp/.ivy2 && \
    pip install boto3

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Build-time arguments (빌드 시 전달)
ARG AWS_ACCESS_KEY_ID
ARG AWS_SECRET_ACCESS_KEY

# 환경 변수 설정
ENV AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
ENV AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY

COPY ./airflow_dags/spark_job/s3_example.py /opt/spark/jobs/s3_example.py