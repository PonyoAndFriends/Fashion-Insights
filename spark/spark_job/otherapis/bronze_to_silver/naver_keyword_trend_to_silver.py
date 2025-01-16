from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    current_date,
    explode,
    monotonically_increasing_id,
)
from pyspark.sql.types import (
    FloatType,
    DateType,
    StringType,
    IntegerType,
    StructType,
    StructField,
    ArrayType,
)
import sys
import logging

logger = logging.getLogger(__name__)

# 스파크 세션 생성
spark = SparkSession.builder.getOrCreate()

# 실행 시 전달받은 인자
args = sys.argv
source_path = args[1] + "/*.json"
target_path = args[2]
gender = args[3]

# JSON 데이터 스키마 정의
schema = StructType(
    [
        StructField("startDate", DateType(), True),
        StructField("endDate", DateType(), True),
        StructField("timeUnit", StringType(), True),
        StructField(
            "results",
            StructType(
                [
                    StructField("keyword", StringType(), True),
                    StructField(
                        "data",
                        ArrayType(
                            StructType(
                                [
                                    StructField("period", DateType(), True),
                                    StructField("ratio", FloatType(), True),
                                ]
                            )
                        ),
                        True,
                    ),
                ]
            ),
            True,
        ),
    ]
)

# JSON 파일 읽기 (스키마 적용)
raw_df = spark.read.schema(schema).json(source_path)

# 기본 데이터 변환
trend_df = raw_df.select(
    monotonically_increasing_id().alias("trend_id"),  # 고유 ID 생성
    col("startDate").alias("start_date"),
    col("endDate").alias("end_date"),
    col("timeUnit").alias("time_unit"),
    lit("패션의류").alias("category_name"),  # 고정 값 추가
    lit("50000000").alias("category_code"),  # 고정 값 추가
    explode(col("results")).alias("result"),  # explode 처리
    lit(gender).alias("gender"),  # gender 값 추가
)

# 데이터 폭발 (explode) 및 변환
result_df = trend_df.select(
    col("trend_id"),
    col("start_date"),
    col("end_date"),
    col("time_unit"),
    col("category_name"),
    col("category_code"),
    col("result.keyword").alias("keyword_name"),
    col("gender"),
    explode(col("result.data")).alias("data"),  # data 리스트 explode
)

# 최종 DataFrame 생성
final_df = result_df.select(
    col("trend_id").cast(IntegerType()),
    col("start_date").cast(DateType()),
    col("end_date").cast(DateType()),
    col("time_unit").cast(StringType()),
    col("category_name").cast(StringType()),
    col("category_code").cast(IntegerType()),
    col("keyword_name").cast(StringType()),
    col("gender").cast(StringType()),
    col("data.period").cast(DateType()).alias("period"),  # 날짜 변환
    col("data.ratio").cast(FloatType()).alias("ratio"),  # 비율 변환
    current_date().cast(DateType()).alias("created_at"),  # 데이터 수집 날짜
)

# Parquet 파일 저장
final_df.repartition(1).write.mode("overwrite").parquet(target_path)

logger.info(f"Processed data saved to: {target_path}")
