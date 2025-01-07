from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
    ArrayType,
)
from pyspark.sql.functions import col
from custom_modules import s3_spark_module
import sys

# SparkSession 생성
spark = SparkSession.builder.appName("JSON to Spark Table").getOrCreate()

# 실행 시 전달받은 인자
args = sys.argv
source_path = args[1]
target_path = args[2]

# JSON 데이터 로드
raw_df = s3_spark_module.read_and_partition_s3_data(spark, source_path, "json")

# JSON 데이터 스키마 정의
schema = StructType(
    [
        StructField("story_id", StringType(), False),
        StructField("content_type", StringType(), False),
        StructField("aggregation_like_count", IntegerType(), False),
        StructField("tags", ArrayType(StringType()), False),
        StructField("created_at", TimestampType(), True),
    ]
)

# 데이터 변환
transformed_df = raw_df.select(
    col("data.list.id").alias("story_id"),
    col("data.list.contentType").alias("content_type"),
    col("data.list.aggregations.likeCount").alias("aggregation_like_count"),
    col("data.list.tags.name").alias("tags"),
    col("data.list.displayedFrom").alias("created_at"),
).withColumn("created_at", col("created_at").cast(TimestampType()))

# 스키마 적용
final_df = spark.createDataFrame(transformed_df.rdd, schema=schema)

# 데이터 저장 또는 출력
final_df.show(truncate=False)

# 데이터 저장 예시 (Parquet 파일로 저장)
final_df.write.mode("overwrite").parquet(target_path)
