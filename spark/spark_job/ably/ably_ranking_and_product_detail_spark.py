import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    regexp_extract,
    input_file_name,
    when,
)
from functools import reduce
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType
from ably_modules.ably_mapping_table import CATEGORY_PARAMS
from datetime import datetime, timedelta

# 로깅 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Spark 세션 생성
spark = SparkSession.builder.appName("Ably Mapping Transformation").getOrCreate()

# 오늘 날짜
today = (datetime.now() + timedelta(hours=9)).strftime("%Y-%m-%d")

# S3 경로
bronze_path = f"s3a://team3-2-s3/bronze/{today}/ably/ranking_data/*/*.json"
silver_product_path = f"s3a://team3-2-s3/silver/{today}/ably/product_detail_tb/"
silver_ranking_path = f"s3a://team3-2-s3/silver/{today}/ably/ranking_tb/"

# 데이터 로드
try:
    raw_data = (
        spark.read.option("multiline", "true")
        .json(bronze_path)
        .withColumn("input_file_name", input_file_name())
    )
    logging.info("데이터 로드 성공")
except Exception as e:
    logging.error(f"데이터 로드 실패: {e}")
    exit(1)

# 파일 경로에서 카테고리 코드 추출
raw_data = raw_data.withColumn(
    "category_code", regexp_extract(col("input_file_name"), r"/([^/]+)\.json$", 1)
)

# 초기화 컬럼 추가
raw_data = raw_data.withColumn("master_category_name", lit("Unknown"))
raw_data = raw_data.withColumn("small_category_name", lit("Unknown"))

# 매핑 적용 함수
def apply_mapping(df, key, value):
    return (
        df.withColumn(
            "master_category_name",
            when(col("category_code") == key, lit(value[0])).otherwise(col("master_category_name")),
        )
        .withColumn(
            "small_category_name",
            when(col("category_code") == key, lit(value[1])).otherwise(col("small_category_name")),
        )
    )

# 매핑 테이블 적용
raw_data = reduce(
    lambda df, key_value: apply_mapping(df, *key_value), CATEGORY_PARAMS.items(), raw_data
)

# 결과 데이터 구성
transformed_data = raw_data.select(
    lit("ably").alias("platform").cast(StringType()),
    col("master_category_name").cast(StringType()),
    col("small_category_name").cast(StringType()),
    col("item.like.goods_sno").alias("product_id").cast(IntegerType()),
    col("item.image").alias("img_url").cast(StringType()),
    col("logging.analytics.GOODS_NAME").alias("product_name").cast(StringType()),
    col("logging.analytics.MARKET_NAME").alias("brand_name_kr").cast(StringType()),
    lit("").alias("brand_name_en").cast(StringType()),
    col("item.first_page_rendering.original_price").alias("original_price").cast(IntegerType()),
    col("logging.analytics.SALES_PRICE").alias("final_price").cast(IntegerType()),
    col("logging.analytics.DISCOUNT_RATE").alias("discount_ratio").cast(IntegerType()),
    col("logging.analytics.REVIEW_COUNT").alias("review_counting").cast(IntegerType()),
    col("logging.analytics.REVIEW_RATING").alias("review_avg_rating").cast(FloatType()),
    col("logging.analytics.LIKES_COUNT").alias("like_counting").cast(IntegerType()),
    lit(today).alias("created_at").cast(DateType()),
)

# ranking_tb 생성
def create_ranking_tb(transformed_data):

    # ranking_tb 생성
    ranking_tb = transformed_data.select(
        col("platform").cast(StringType()),
        col("master_category_name").cast(StringType()),
        col("product_id").cast(IntegerType()),
        col("ranking").cast(IntegerType()),
        col("created_at"),
    )

    return ranking_tb

ranking_tb = create_ranking_tb(transformed_data)

# ranking_tb 저장
try:
    ranking_tb.write.mode("overwrite").parquet(silver_ranking_path)
    logging.info("ranking_tb 저장 성공")
except Exception as e:
    logging.error(f"ranking_tb 저장 실패: {e}")

# product_detail_tb 저장
try:
    transformed_data.write.mode("overwrite").parquet(silver_product_path)
    logging.info("product_detail_tb 저장 성공")
except Exception as e:
    logging.error(f"product_detail_tb 저장 실패: {e}")
