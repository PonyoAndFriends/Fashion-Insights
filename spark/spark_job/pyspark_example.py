from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, IntegerType

# Spark 세션 생성
spark = SparkSession.builder.appName("S3ReadWrite").getOrCreate()

# JSON 스키마 정의
schema = StructType([StructField("start", IntegerType(), True)])

# S3에서 데이터 읽기
input_path = "s3a://source-bucket-hs/input-data.json"
output_path = "s3a://destination-bucket-hs/output-data.json"

df = spark.read.option("multiLine", "false").schema(schema).json(input_path)

# 데이터 처리 (예: 'processed' 열 추가)
df.show()

# 데이터 처리 예제
processed_df = df.withColumn("another", df["start"] + 1)

# S3에 결과 저장
processed_df.write.mode("overwrite").json(output_path)

spark.stop()
