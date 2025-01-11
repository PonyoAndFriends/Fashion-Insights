from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, substring, when
from pyspark.sql.types import FloatType
from datetime import datetime, timedelta
import logging
import os, sys

args = sys.argv
BUCKET_NAME = args[0]
AWS_ACCESS_KEY = args[1]
AWS_SECRET_KEY = args[2]

# Spark 세션 생성
spark = (
    SparkSession.builder.appName("Review Data Transformation")
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .getOrCreate()
)

today = (datetime.now() + timedelta(hours=9)).strftime("%Y-%m-%d")

# JSON 데이터 경로
review_data_path = f"s3a://{BUCKET_NAME}/bronze/{today}/29cm/29cm_reviews/*/*/*_reviews.json"

# 데이터 로드
try:
    raw_data = spark.read.option("multiline", "true").json(review_data_path)
    logging.info("리뷰 데이터 로드 성공")
except Exception as e:
    logging.error(f"리뷰 데이터 로드 실패: {e}")
    exit(1)

# 테이블 변환
transformed_data = raw_data.select(
    col("product_id").cast("int").alias("product_id"),
    col("contents").alias("review_content"),
    col("point").cast("int").alias("review_rating"),
    substring(col("insertTimestamp"), 1, 10).alias("review_date"),
    regexp_replace(col("userSize").getItem(0), "cm", "")
    .cast(FloatType())
    .alias("reviewer_height"),
    regexp_replace(col("userSize").getItem(1), "kg", "")
    .cast(FloatType())
    .alias("reviewer_weight"),
    when(col("optionValue").isNotNull(), col("optionValue").getItem(0)).alias(
        "selected_options"
    ),
    col("created_at").alias("created_at"),  # created_at 유지
)

# 저장 경로
output_path = (
    f"s3a://{BUCKET_NAME}/silver/{today}/29cm/29cm_review_detail_tb/"
)

try:
    transformed_data.write.mode("overwrite").parquet(output_path)
    logging.info(f"Platform_product_review_detail_tb 저장 완료: {output_path}")
except Exception as e:
    logging.error(f"데이터 저장 실패: {e}")