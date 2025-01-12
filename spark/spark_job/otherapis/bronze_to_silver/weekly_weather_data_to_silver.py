from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, trim, expr
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    IntegerType,
    DateType,
)
import sys
import logging

logger = logging.getLogger(__name__)

# 스파크 세션 생성
spark = SparkSession.builder.appName("Weather Data Processing").getOrCreate()

# 실행 시 전달받은 인자
args = sys.argv
source_path = args[1]
target_path = args[2]

# 텍스트 파일 읽기
raw_rdd = (
    spark.read.text(source_path)  # S3에서 텍스트 파일 읽기
    .rdd
    .map(lambda row: row[0].strip())  # 공백 제거
)

# START7777과 END7777 사이의 데이터 필터링
filtered_rdd = raw_rdd.filter(
    lambda line: line and not line.startswith("#") and "START7777" not in line and "END7777" not in line
)

# 데이터를 공백으로 구분하고 RDD를 데이터프레임으로 변환
split_rdd = filtered_rdd.map(lambda line: line.split())

# 스키마 정의
schema = StructType(
    [
        StructField("STN_ID", IntegerType(), True),
        StructField("LON", FloatType(), True),
        StructField("LAT", FloatType(), True),
        StructField("STN_SP", StringType(), True),
        StructField("HT", FloatType(), True),
        StructField("HT_PA", FloatType(), True),
        StructField("HT_TA", FloatType(), True),
        StructField("HT_WD", FloatType(), True),
        StructField("HT_RN", FloatType(), True),
        StructField("STN_AD", IntegerType(), True),
        StructField("STN_KO", StringType(), True),
        StructField("STN_EN", StringType(), True),
        StructField("FCT_ID", StringType(), True),
        StructField("LAW_ID", StringType(), True),
        StructField("BASIN", StringType(), True),
    ]
)

# 데이터프레임 생성
data_df = spark.createDataFrame(split_rdd, schema)

# 데이터 변환 (필요한 컬럼의 타입 변환 및 추가 처리)
processed_df = data_df.withColumn("LON", col("LON").cast(FloatType())).withColumn("LAT", col("LAT").cast(FloatType()))

# 결과 저장
processed_df.write.mode("overwrite").parquet(target_path)

logger.info(f"Data processed and saved to {target_path}")
