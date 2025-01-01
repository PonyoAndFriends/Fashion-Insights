from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# SparkSession 생성
spark = SparkSession.builder \
    .appName("S3JSONProcessing") \
    .getOrCreate()

# S3 경로 설정
input_path = "s3a://source-bucket-hs/input.json"
output_path = "s3a://destination-bucket-hs/output.json"

# S3에서 JSON 파일 읽기
df = spark.read.json(input_path)

# 데이터 변환
df_transformed = df.withColumn("another", lit(1))

# 결과 저장
df_transformed.write.json(output_path, mode="overwrite")
