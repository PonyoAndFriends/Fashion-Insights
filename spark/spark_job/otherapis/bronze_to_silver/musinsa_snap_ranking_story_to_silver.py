from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType,
    DateType,
)
from pyspark.sql.functions import col, lit, explode, expr
import logging
import sys

logger = logging.getLogger(__name__)

# SparkSession 생성
spark = SparkSession.builder.appName("snap_ranking_story_to_silver_s3").getOrCreate()

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
        StructField("gender", StringType(), True),
        StructField("created_at", DateType(), True),
    ]
)

# JSON 배열을 explode로 변환
exploded_df = raw_json_df.selectExpr("explode(data.list) as list_item")

# 데이터 변환
transformed_df = exploded_df.select(
    col("list_item.id").alias("story_id"),
    col("list_item.contentType").alias("content_type"),
    col("list_item.aggregations.likeCount").alias("aggregation_like_count"),
    # tags 배열에서 각 태그의 name 필드 추출
    expr("transform(list_item.tags, tag -> tag.name)").alias("tags"),
    col("list_item.displayedFrom").alias("created_at"),
    lit("남성" if gender == "MEN" else "여성").alias("gender"),
).withColumn("created_at", col("created_at").cast(DateType()))

# 스키마 적용
final_df = spark.createDataFrame(transformed_df.rdd, schema=schema)

# 데이터 저장 (Parquet 파일로 저장)
final_df.write.mode("overwrite").parquet(target_path)

logger.info(f"Processed JSON data has been saved to: {target_path}")
