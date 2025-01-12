from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    IntegerType,
    DateType,
)
import logging
import sys

logger = logging.getLogger(__name__)

# Spark 세션 생성
spark = SparkSession.builder.appName("Weather Data Processing").getOrCreate()

# 실행 시 전달받은 인자
args = sys.argv
source_path = args[1]
target_path = args[2]

# 데이터 읽기
raw_rdd = (
    spark.read.text(source_path)
    .rdd.map(lambda row: row[0].strip())  # 공백 제거
)

# START7777, END7777, 주석 제거
filtered_rdd = raw_rdd.filter(
    lambda line: line and not line.startswith("#") and "START7777" not in line and "END7777" not in line
)

# 데이터 공백 기준으로 분리
split_rdd = filtered_rdd.map(lambda line: line.split(","))

# 스키마 정의
schema = StructType(
    [
        StructField("TM", StringType(), True),  # 관측일
        StructField("STN", IntegerType(), True),  # 지점번호
        StructField("WS_AVG", FloatType(), True),  # 평균풍속
        StructField("WS_MAX", FloatType(), True),  # 최대풍속
        StructField("TA_AVG", FloatType(), True),  # 평균기온
        StructField("TA_MAX", FloatType(), True),  # 최고기온
        StructField("TA_MIN", FloatType(), True),  # 최저기온
        StructField("HM_AVG", FloatType(), True),  # 평균습도
        StructField("HM_MIN", FloatType(), True),  # 최저습도
        StructField("FG_DUR", FloatType(), True),  # 안개시간
        StructField("CA_TOT", FloatType(), True),  # 전운량
        StructField("RN_DAY", FloatType(), True),  # 일 강수량
        StructField("RN_DUR", FloatType(), True),  # 강수계속시간
        StructField("RN_60M_MAX", FloatType(), True),  # 1시간 최다강수량
        StructField("RN_POW_MAX", FloatType(), True),  # 최대 강우강도
    ]
)

# 필요한 컬럼만 추출 및 변환
data_rdd = split_rdd.map(
    lambda row: (
        row[0],  # TM
        int(row[1]),  # STN
        float(row[2]),  # WS_AVG
        float(row[5]),  # WS_MAX
        float(row[10]),  # TA_AVG
        float(row[11]),  # TA_MAX
        float(row[13]),  # TA_MIN
        float(row[18]),  # HM_AVG
        float(row[19]),  # HM_MIN
        float(row[24]),  # FG_DUR
        float(row[31]),  # CA_TOT
        float(row[38]),  # RN_DAY
        float(row[40]),  # RN_DUR
        float(row[42]),  # RN_60M_MAX
        float(row[46]),  # RN_POW_MAX
    )
)

# RDD를 데이터프레임으로 변환
data_df = spark.createDataFrame(data_rdd, schema=schema)

# 데이터 검증 및 결측값 처리
processed_df = (
    data_df
    .fillna({
        "WS_AVG": 0.0, "WS_MAX": 0.0, "TA_AVG": 0.0,
        "TA_MAX": 0.0, "TA_MIN": 0.0, "HM_AVG": 0.0,
        "HM_MIN": 0.0, "FG_DUR": 0.0, "CA_TOT": 0.0,
        "RN_DAY": 0.0, "RN_DUR": 0.0, "RN_60M_MAX": 0.0,
        "RN_POW_MAX": 0.0,
    })
    .withColumn("TM", col("TM").cast(DateType()))  # TM을 날짜 타입으로 변환
)

# 결과 저장
processed_df.write.mode("overwrite").parquet(target_path)
logger.info(f"Data processed and saved to {target_path}")
