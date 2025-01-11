from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, trim
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    IntegerType,
    DateType,
)
from custom_modules import s3_spark_module
import sys
import logging

logger = logging.getLogger(__name__)

# 스파크 세션 생성
spark = SparkSession.builder.getOrCreate()

# 실행 시 전달받은 인자
args = sys.argv
source_path = args[1] + "/*.txt"
target_path = args[2]

# 텍스트 파일 읽기
raw_df = s3_spark_module.read_and_partition_s3_data(spark, source_path, "txt")

# START7777과 END7777 사이의 데이터 필터링 (주석 및 빈 줄 제거)
filtered_df = raw_df.filter(
    (col("value").strip() != "")  # 빈 줄 제거
    & (~col("value").startswith("#"))  # 주석 제거
    & (~col("value").startswith("START7777"))  # START 제거
    & (~col("value").startswith("END7777"))  # END 제거
)

# 데이터를 쉼표(,)로 분리하고 각 컬럼 매핑
split_df = filtered_df.withColumn("columns", split(trim(col("value")), ","))

# 스키마 정의
schema = StructType(
    [
        StructField("TM", StringType(), True),  # 관측일
        StructField("STN", IntegerType(), True),  # 국내 지점번호
        StructField("WS_AVG", FloatType(), True),  # 일 평균 풍속
        StructField("WS_MAX", FloatType(), True),  # 최대풍속
        StructField("TA_AVG", FloatType(), True),  # 일 평균기온
        StructField("TA_MAX", FloatType(), True),  # 최고기온
        StructField("TA_MIN", FloatType(), True),  # 최저기온
        StructField("HM_AVG", FloatType(), True),  # 일 평균 상대습도
        StructField("HM_MIN", FloatType(), True),  # 최저습도
        StructField("FG_DUR", FloatType(), True),  # 안개계속시간
        StructField("CA_TOT", FloatType(), True),  # 일 평균 전운량
        StructField("RN_DAY", FloatType(), True),  # 일 강수량
        StructField("RN_DUR", FloatType(), True),  # 강수계속시간
        StructField("RN_60M_MAX", FloatType(), True),  # 1시간 최다강수량
        StructField("RN_POW_MAX", FloatType(), True),  # 최대 강우강도
    ]
)

# 스키마를 적용하여 데이터프레임 생성
data_df = split_df.select(
    col("columns")[0].cast(StringType()).alias("TM"),
    col("columns")[1].cast(IntegerType()).alias("STN"),
    col("columns")[2].cast(FloatType()).alias("WS_AVG"),
    col("columns")[3].cast(FloatType()).alias("WS_MAX"),
    col("columns")[4].cast(FloatType()).alias("TA_AVG"),
    col("columns")[5].cast(FloatType()).alias("TA_MAX"),
    col("columns")[6].cast(FloatType()).alias("TA_MIN"),
    col("columns")[7].cast(FloatType()).alias("HM_AVG"),
    col("columns")[8].cast(FloatType()).alias("HM_MIN"),
    col("columns")[9].cast(FloatType()).alias("FG_DUR"),
    col("columns")[10].cast(FloatType()).alias("CA_TOT"),
    col("columns")[11].cast(FloatType()).alias("RN_DAY"),
    col("columns")[12].cast(FloatType()).alias("RN_DUR"),
    col("columns")[13].cast(FloatType()).alias("RN_60M_MAX"),
    col("columns")[14].cast(FloatType()).alias("RN_POW_MAX"),
)

# 필요한 컬럼의 데이터 변환
processed_df = data_df.withColumn("TM", col("TM").cast(DateType()))

# 결과 저장
processed_df.write.mode("overwrite").parquet(target_path)

logger.info(f"Data processed and saved to {target_path}")
