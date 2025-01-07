from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys

# Spark 세션 생성
spark = SparkSession.builder.appName("KMADataProcessing").getOrCreate()

# 실행 시 전달받은 인자
args = sys.argv
source_path = args[1]
target_path = args[2]

# 텍스트 파일 읽기
raw_df = spark.read.text(source_path)

# START7777와 7777END 사이의 데이터만 필터링
filtered_df = raw_df.filter(~col("value").startswith("#")).filter(col("value").rlike(r"^\s*\d+"))

# 데이터를 파싱
parsed_df = filtered_df.selectExpr(
    "CAST(split(value, '\\s+')[0] AS INT) AS STN",
    "CAST(split(value, '\\s+')[1] AS FLOAT) AS LON",
    "CAST(split(value, '\\s+')[2] AS FLOAT) AS LAT",
    "CAST(split(value, '\\s+')[3] AS INT) AS STN_SP",
    "CAST(split(value, '\\s+')[4] AS FLOAT) AS HT",
    "CAST(split(value, '\\s+')[5] AS FLOAT) AS HT_PA",
    "CAST(split(value, '\\s+')[6] AS FLOAT) AS HT_TA",
    "CAST(split(value, '\\s+')[7] AS FLOAT) AS HT_WD",
    "CAST(split(value, '\\s+')[8] AS FLOAT) AS HT_RN",
    "CAST(split(value, '\\s+')[9] AS STRING) AS STN_KO",
    "CAST(split(value, '\\s+')[10] AS STRING) AS STN_EN"
)

# 결과 출력
parsed_df.show(truncate=False)

# 필요한 테이블 형태로 변환
final_df = parsed_df.select(
    col("STN"),
    # 날짜(TIME) 추가 필요 시 컬럼 생성 가능
    col("LON").alias("WS_AVG"),
    col("LAT").alias("WS_MAX"),
    col("HT").alias("TA_AVG"),
    col("HT_PA").alias("TA_MAX"),
    col("HT_TA").alias("TA_MIN"),
    col("HT_WD").alias("HM_AVG"),
    col("HT_RN").alias("HM_MIN")
    # 다른 컬럼 필요 시 추가
)

# 결과 저장 (S3로)
output_path = "s3://your-bucket-name/output/"
final_df.write.mode("overwrite").csv(output_path)
