from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType,
    TimestampType,
)
from pyspark.sql.functions import col, lit
from custom_modules import s3_spark_module
import sys
import logging

logger = logging.getLogger(__name__)

# SparkSession 생성
spark = SparkSession.builder.appName("snap_ranking_story_to_silver_s3").getOrCreate()

# 실행 시 전달받은 인자
args = sys.argv
source_path = args[1] + "/*.json"
target_path = args[2]
gender = args[3]

# JSON 데이터 로드
raw_json_df = s3_spark_module.read_and_partition_s3_data(spark, source_path, "json")

# JSON 데이터 스키마 정의
schema = StructType(
    [
        StructField("story_id", StringType(), False),
        StructField("content_type", StringType(), False),
        StructField("aggregation_like_count", IntegerType(), False),
        StructField("tags", ArrayType(StringType()), False),
        StructField("gender", StringType(), True),
        StructField("created_at", TimestampType(), True),
    ]
)

# 데이터 변환
transformed_df = raw_json_df.select(
    col("data.list.id").alias("story_id"),
    col("data.list.contentType").alias("content_type"),
    col("data.list.aggregations.likeCount").alias("aggregation_like_count"),
    col("data.list.tags.name").alias("tags"),
    col("data.list.displayedFrom").alias("created_at"),
    lit("남성" if gender == "MEN" else "여성").alias("gender"),
).withColumn("created_at", col("created_at").cast(TimestampType()))

# 스키마 적용
final_df = spark.createDataFrame(transformed_df.rdd, schema=schema)

# 데이터 저장 예시 (Parquet 파일로 저장)
final_df.write.mode("overwrite").parquet(target_path)

logger.info(f"Processed JSON data has been saved to: {target_path}")
