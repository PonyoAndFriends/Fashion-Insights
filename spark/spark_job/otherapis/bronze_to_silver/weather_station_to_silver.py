from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, trim
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from custom_modules import s3_spark_module
import sys
import logging

logger = logging.getLogger(__name__)

# Spark 세션 생성
spark = SparkSession.builder.appName("WeatherDataProcessing").getOrCreate()

# 실행 시 전달받은 인자
args = sys.argv
source_path = args[1]
target_path = args[2]

# 텍스트 파일 읽기
raw_df = s3_spark_module.read_and_partition_s3_data(spark, source_path, "txt")

# START7777와 END7777 사이 데이터 필터링
filtered_df = raw_df.filter(
    (col("value").strip() != "")  # 빈 줄 제거
    & (~col("value").startswith("#"))  # 주석 제거
    & (~col("value").contains("START7777"))  # START 제거
    & (~col("value").contains("7777END"))  # END 제거
)

# 데이터를 공백으로 분리하고 컬럼 생성
parsed_df = filtered_df.withColumn("columns", split(trim(col("value")), r"\s+"))

# 스키마 정의 (필요한 모든 컬럼 정의)
schema = StructType(
    [
        StructField("STN_ID", IntegerType(), False),  # 지점번호 (PK, NULL 불가)
        StructField("STN_KO", StringType(), True),   # 지점명(한글)
    ]
)

# 데이터를 스키마에 맞게 매핑
data_df = parsed_df.select(
    col("columns")[0].cast(IntegerType()).alias("STN_ID"),  # STN_ID
    col("columns")[10].alias("STN_KO"),                    # STN_KO
)

# 스키마 적용 (데이터 검증)
final_df = spark.createDataFrame(data_df.rdd, schema)

# 결과 저장
final_df.write.mode("overwrite").parquet(target_path)

logger.info(f"Data processed and saved to {target_path}")
