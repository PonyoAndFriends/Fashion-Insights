from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    explode,
    collect_list,
    current_timestamp,
    flatten,
    count,
    desc,
    row_number
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType,
    TimestampType,
)
from pyspark.sql.window import Window
from custom_modules import s3_spark_module
import sys
import logging

logger = logging.getLogger(__name__)

# 스파크 세션 생성
spark = SparkSession.builder.getOrCreate()

# 실행 시 전달받은 인자
args = sys.argv
source_path = args[1] + "/*.json"
target_path = args[2]
logger.info(f"source_path: {source_path}")
logger.info(f"target_path: {target_path}")

# S3에서 모든 JSON 파일 읽기
raw_json_df = spark.read.json(source_path)

# JSON 처리 - 필요한 컬럼 추출 및 테이블 스키마에 맞게 변환
brands_df = (
    raw_json_df.selectExpr("data.list")
    .withColumn("list", explode(col("list")))
    .select("list.*")
)

table_df = brands_df.select(
    col("id").alias("brand_id"),
    col("nickname").alias("brand_name"),
    col("profileImageUrl").alias("img_url"),
    col("ranking.rank").alias("rank"),
    col("ranking.previousRank").alias("previous_rank"),
    col("followerCount").alias("follower_count"),
    col("snaps.labels").alias("labels"),
).withColumn("created_at", current_timestamp())

# Extracting categoryName from labels
category_names_df = table_df.withColumn("label", explode("labels")).select(
    col("brand_id"), col("label.name").alias("name")
)

# Grouping category names into a single list per brand_id
category_names_grouped_df = category_names_df.groupBy("brand_id").agg(
    collect_list("name").alias("name")
)

# Flattening the list without removing duplicates
category_names_grouped_df = category_names_grouped_df.withColumn(
    "label_names", flatten(col("name"))
)

# Join with the original table
table_with_categories = table_df.join(category_names_grouped_df, "brand_id", "left").select(
    "brand_id",
    "brand_name",
    "img_url",
    "rank",
    "previous_rank",
    "follower_count",
    "label_names",
    "created_at",
)

# JSON 데이터 스키마 정의
schema = StructType(
    [
        StructField("brand_id", StringType(), True),
        StructField("brand_name", StringType(), True),
        StructField("img_url", StringType(), True),
        StructField("rank", IntegerType(), True),
        StructField("previous_rank", IntegerType(), True),
        StructField("follower_count", IntegerType(), True),
        StructField("label_names", ArrayType(StringType()), True),
        StructField("created_at", TimestampType(), True),
    ]
)

# 스키마 적용
final_table_with_schema = spark.createDataFrame(
    table_with_categories.rdd, schema=schema
).repartition(1)

# 결과를 S3 대상 경로로 저장
final_table_with_schema.write.mode("overwrite").parquet(target_path)

# 완료 메시지
logger.info(f"Processed JSON data has been saved to: {target_path}")
