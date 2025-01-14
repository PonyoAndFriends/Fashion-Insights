from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType,
    DateType,
)
from pyspark.sql.functions import col, lit, expr, current_date, to_json
import logging
import sys

logger = logging.getLogger(__name__)

# SparkSession 생성
spark = SparkSession.builder.getOrCreate()

# 실행 시 전달받은 인자
args = sys.argv
source_path = args[1]
target_path = args[2]
gender = args[3]

# JSON 데이터 로드
raw_json_df = spark.read.json(source_path)

# JSON 데이터 스키마 정의
schema = StructType(
    [
        StructField("story_id", StringType(), False),
        StructField("content_type", StringType(), False),
        StructField("aggregation_like_count", IntegerType(), False),
        StructField("tags", ArrayType(StringType()), False),
        StructField("created_at", DateType(), True),
        StructField("gender", StringType(), True),
    ]
)

# JSON 배열을 explode로 변환
exploded_df = raw_json_df.selectExpr("explode(data.list) as list_item")


transformed_df = exploded_df.select(
    col("list_item.id").alias("story_id"),
    col("list_item.contentType").alias("content_type"),
    col("list_item.aggregations.likeCount").alias("aggregation_like_count"),
    expr("transform(list_item.tags, tag -> tag.name)").alias("tags"),  # ARRAY로 변환
    lit("남성").alias("gender"),
).withColumn("created_at", current_date())

# tags 열을 JSON 문자열로 변환
transformed_df = transformed_df.withColumn("tags", to_json(col("tags")))

final_df = transformed_df.select(
    "story_id", "content_type", "aggregation_like_count", "tags", "created_at", "gender"
)

# 데이터 저장 (Parquet 파일로 저장)
final_df.write.mode("overwrite").parquet(target_path)

logger.info(f"Processed JSON data has been saved to: {target_path}")
