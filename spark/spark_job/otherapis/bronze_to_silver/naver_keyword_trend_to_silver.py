from pyspark.sql import SparkSession
from pyspark.sql.window import Window

from pyspark.sql.functions import col, lit, current_date, explode, monotonically_increasing_id
from pyspark.sql.types import (
    FloatType,
    DateType,
    IntegerType,
    StringType,
    StructType,
    StructField,
    DateType,
)
from custom_modules import s3_spark_module
from datetime import datetime
import sys, json
import logging

logger = logging.getLogger(__name__)

# 스파크 세션 생성
spark = SparkSession.builder.getOrCreate()

# 실행 시 전달받은 인자
args = sys.argv
source_path = args[1] + "/*.json"
target_path = args[2]
gender = args[3]  # gender 값을 명시적으로 전달받음

# JSON 파일 읽기
raw_df = spark.read.json(source_path)

# 기본 데이터 변환
trend_df = raw_df.select(
    lit(None).cast(IntegerType()).alias("trend_id"),  # trend_id placeholder
    col("startDate").cast(DateType()).alias("start_date"),
    col("endDate").cast(DateType()).alias("end_date"),
    col("timeUnit").alias("time_unit"),
    explode(col("results")).alias("result")
)

# 데이터 폭발 (explode) 및 변환
result_df = trend_df.select(
    monotonically_increasing_id().alias("trend_id"),  # 고유 ID 생성
    col("start_date"),
    col("end_date"),
    col("time_unit"),
    lit("패션의류").alias("category_name"),  # 고정 값 추가
    lit("50000000").alias("category_code"),  # 고정 값 추가
    col("result.keyword").getItem(0).alias("keyword_name"),
    lit(None).cast(StringType()).alias("gender"),  # NULL로 초기화
    explode(col("result.data")).alias("data")  # data 리스트 explode
)

# 최종 데이터 프레임 생성
final_df = result_df.select(
    col("trend_id"),
    col("start_date"),
    col("end_date"),
    col("time_unit"),
    col("category_name"),
    col("category_code"),
    col("keyword_name"),
    col("gender"),
    col("data.period").cast(DateType()).alias("period"),  # 날짜 변환
    col("data.ratio").cast(FloatType()).alias("ratio"),  # 비율 변환
    current_date().alias("created_at")  # 데이터 수집 날짜
)

# 스키마를 적용한 최종 DataFrame 생성
final_df = final_df.repartition(1)

# Parquet 형식으로 저장
final_df.write.mode("overwrite").parquet(target_path)

logger.info(f"Processed data saved to: {target_path}")
