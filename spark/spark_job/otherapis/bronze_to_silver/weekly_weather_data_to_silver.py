from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf, regexp_extract
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, DateType

# 스파크 세션 생성
spark = SparkSession.builder.appName("Weather Data Processing").getOrCreate()

# S3 텍스트 파일 경로
input_path = "s3://source-bucket-name/path-to-txt-files/"
output_path = "s3://destination-bucket-name/output-path/"

# 텍스트 파일 읽기
raw_rdd = spark.sparkContext.textFile(input_path)

# START7777과 END 사이의 데이터 추출
data_rdd = raw_rdd.filter(lambda line: not line.startswith("#") and line.strip() != "").filter(
    lambda line: not line.startswith("#START") and not line.startswith("#END")
)

# RDD를 DataFrame으로 변환
schema = StructType([
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
])

# 데이터를 쉼표(,)로 분리하고 필드에 매핑
data_df = spark.createDataFrame(
    data_rdd.map(lambda line: line.split(",")),
    schema=schema
)

# 필요한 컬럼의 데이터 변환
processed_df = data_df.withColumn("TM", col("TM").cast(DateType()))

# 결과 저장
processed_df.write.mode("overwrite").parquet(output_path)
