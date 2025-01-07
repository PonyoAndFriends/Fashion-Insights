from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, explode, input_file_name, regexp_extract
from pyspark.sql.types import FloatType, DateType, IntegerType
import sys

# 스파크 세션 생성
spark = SparkSession.builder \
    .appName("Process Trend Data") \
    .getOrCreate()

# 실행 시 전달받은 인자
args = sys.argv
source_path = args[1]
target_path = args[2]

# JSON 파일 읽기
raw_df = spark.read.json(source_path, multiLine=True)

# 파일 경로에서 gender 추출
raw_df = raw_df.withColumn("gender", regexp_extract(input_file_name(), r"/([^/]+)_keyword_trends/", 1))


# 데이터 가공
# 트렌드 ID 생성 (auto-increment처럼 동작)
processed_df = (
    raw_df.select(
        lit(None).cast(IntegerType()).alias("trend_id"),  # trend_id placeholder
        col("startDate").cast(DateType()).alias("start_date"),
        col("endDate").cast(DateType()).alias("end_date"),
        col("timeUnit").alias("time_unit"),
        explode(col("results")).alias("result")
    )
    .select(
        col("trend_id"),
        col("start_date"),
        col("end_date"),
        col("time_unit"),
        col("result.title").alias("title"),
        col("result.keyword")[0].alias("keyword_name"),
        explode(col("result.data")).alias("data")
    )
    .select(
        col("trend_id"),
        col("start_date"),
        col("end_date"),
        col("time_unit"),
        col("title"),
        col("keyword_name"),
        col("data.period").cast(DateType()).alias("period"),
        col("data.ratio").cast(FloatType()).alias("ratio"),
        lit("패션의류").alias("category_name"),
        lit("50000000").alias("category_code"),
        col("gender"),
        current_timestamp().alias("created_at")  # 데이터 수집 시간 추가
    )
)

# Parquet 형식으로 저장
processed_df.write.mode("overwrite").parquet(target_path)
