from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# SparkSession 생성
spark = SparkSession.builder \
    .appName("S3 Example") \
    .config("spark.hadoop.fs.s3a.access.key", "YOUR_ACCESS_KEY") \
    .config("spark.hadoop.fs.s3a.secret.key", "YOUR_SECRET_KEY") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
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
