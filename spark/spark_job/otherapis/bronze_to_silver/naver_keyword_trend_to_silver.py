from pyspark.sql import SparkSession
from pyspark.sql.window import Window

from pyspark.sql.functions import col, lit, current_date, explode, monotonically_increasing_id, row_number, when
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

# 기본 컬럼 변환
trend_df = raw_df.select(
    lit(None).cast(IntegerType()).alias("trend_id"),  # trend_id placeholder
    col("startDate").cast(DateType()).alias("start_date"),
    col("endDate").cast(DateType()).alias("end_date"),
    col("timeUnit").alias("time_unit"),
    lit(gender).alias("gender"),  # gender 값 전달받아 추가
    explode(col("results")).alias("result"),
).withColumn("trend_id", monotonically_increasing_id())

# 과거 7일치 날짜 생성
dates_df = spark.sql("SELECT sequence(date_sub(current_date(), 6), current_date()) as dates")

# explode를 사용하여 ARRAY<DATE>를 전개
exploded_dates_df = dates_df.selectExpr("explode(dates) as period")

# 기본값 데이터 생성
seven_days_data = [{"period": row["period"], "ratio": 0.0} for row in exploded_dates_df.collect()]

seven_days_data_json = json.dumps(seven_days_data)

trend_df = trend_df.withColumn(
    "result.data",
    when(
        (col("result.data").isNull()) | (col("result.data") == lit([])),
        lit(seven_days_data_json)  # JSON 문자열로 사용
    ).otherwise(col("result.data"))
)

# 순차적 ID 부여
window_spec = Window.orderBy(lit(1))  # 특정 정렬 기준(예: 전체 데이터)에 따라 순서 부여
trend_df = trend_df.withColumn("trend_id", row_number().over(window_spec))

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
    current_date().alias("created_at"),  # 데이터 수집 시간
)

# 스키마를 적용한 최종 DataFrame 생성
final_df = final_df.repartition(1)

# Parquet 형식으로 저장
final_df.write.mode("overwrite").parquet(target_path)

logger.info(f"Processed data saved to: {target_path}")
