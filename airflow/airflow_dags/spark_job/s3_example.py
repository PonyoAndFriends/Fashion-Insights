from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import os

# SparkSession 생성
spark = SparkSession.builder \
    .appName("IAM Role S3 Example") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.WebIdentityTokenCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# S3에서 데이터 읽기
input_path = "s3a://source-bucket-hs/input-data.json"
output_path = "s3a://destination-bucket-hs/output-data.json"
data = spark.read.json(input_path)

# 데이터 변환 (예: 컬럼 추가)
df_with_new_field = data.withColumn("end", lit(1))

# 변환된 데이터 S3에 저장
data.write.json(output_path)

spark.stop()
