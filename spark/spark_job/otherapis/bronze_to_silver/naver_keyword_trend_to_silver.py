from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, explode
from pyspark.sql.types import (
    FloatType,
    DateType,
    IntegerType,
    StringType,
    StructType,
    StructField,
    TimestampType,
)
from custom_modules import s3_spark_module
import sys, logging

logger = logging.getLogger(__name__)

# 스파크 세션 생성
spark = SparkSession.builder.appName("naver_keyword_trend_to_silver_s3").getOrCreate()

# 실행 시 전달받은 인자
args = sys.argv
source_path = args[1]
target_path = args[2]
gender = args[3]  # gender 값을 명시적으로 전달받음

# JSON 파일 읽기
raw_df = s3_spark_module.read_and_partition_s3_data(spark, source_path, "json")

# 기본 컬럼 변환
trend_df = raw_df.select(
    lit(None).cast(IntegerType()).alias("trend_id"),  # trend_id placeholder
    col("startDate").cast(DateType()).alias("start_date"),
    col("endDate").cast(DateType()).alias("end_date"),
    col("timeUnit").alias("time_unit"),
    lit(gender).alias("gender"),  # gender 값 전달받아 추가
    explode(col("results")).alias("result"),
)

# results 컬럼 데이터 변환
result_df = trend_df.select(
    col("trend_id"),
    col("start_date"),
    col("end_date"),
    col("time_unit"),
    col("gender"),
    col("result.keyword").getItem(0).alias("keyword_name"),  # 첫 번째 keyword 가져오기
    explode(col("result.data")).alias("data"),  # data 리스트 explode
)

# data 컬럼 데이터 변환 및 추가 컬럼 설정
final_df = result_df.select(
    col("trend_id"),
    col("start_date"),
    col("end_date"),
    col("time_unit"),
    col("gender"),
    col("keyword_name"),
    col("data.period").cast(DateType()).alias("period"),  # 날짜 변환
    col("data.ratio").cast(FloatType()).alias("ratio"),  # 비율 변환
    lit("패션의류").alias("category_name"),  # 고정 값 추가
    lit("50000000").alias("category_code"),  # 고정 코드 추가
    current_timestamp().alias("created_at"),  # 데이터 수집 시간
)

# 스키마 정의
schema = StructType(
    [
        StructField("trend_id", IntegerType(), True),
        StructField("start_date", DateType(), False),
        StructField("end_date", DateType(), False),
        StructField("time_unit", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("title", StringType(), False),
        StructField("keyword_name", StringType(), False),
        StructField("period", DateType(), False),
        StructField("ratio", FloatType(), False),
        StructField("category_name", StringType(), False),
        StructField("category_code", StringType(), False),
        StructField("created_at", TimestampType(), False),
    ]
)

# 스키마를 적용한 최종 DataFrame 생성
final_df_with_schema = spark.createDataFrame(final_df.rdd, schema=schema)

# Parquet 형식으로 저장
final_df_with_schema.write.mode("overwrite").parquet(target_path)

logger.info(f"Processed data saved to: {target_path}")
