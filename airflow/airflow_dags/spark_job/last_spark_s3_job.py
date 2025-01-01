from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Spark 세션 생성
spark = SparkSession.builder.appName("S3ReadWrite").getOrCreate()

# S3에서 데이터 읽기
input_path = "s3a://your-input-bucket/input-data.json"
output_path = "s3a://your-output-bucket/output-data/"

df = spark.read.json(input_path)

# 데이터 처리 (예: 'processed' 열 추가)
processed_df = df.withColumn("processed", lit("true"))

# S3에 결과 저장
processed_df.write.mode("overwrite").json(output_path)

spark.stop()
