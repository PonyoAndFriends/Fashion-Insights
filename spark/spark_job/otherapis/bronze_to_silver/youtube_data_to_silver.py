from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import sys

# Spark 세션 생성
spark = SparkSession.builder.appName("YouTubeDataProcessing").getOrCreate()

# 실행 시 전달받은 인자
args = sys.argv
source_path = args[1]
target_path = args[2]

raw_df = spark.read.json(source_path)

# 데이터 스키마 정의 및 변환
processed_df = raw_df.select(
    col("videoId").alias("video_id"),
    col("category_name"),  
    col("channelName").alias("channel_title"),
    col("title"),
    col("thumbnailUrl").alias("img_url"),
    col("duration").alias("duration_seconds"),
    col("publishedAt").cast(TimestampType()).alias("published_at"),
    col("viewCount").cast(IntegerType()).alias("view_count"),
    col("likeCount").cast(IntegerType()).alias("like_count"),
    current_timestamp().alias("created_at")  # 데이터 수집 날짜
)

# 출력 테이블 확인
processed_df.show(truncate=False)

# 스키마 적용
schema = StructType([
    StructField("video_id", StringType(), False),
    StructField("category_name", StringType(), False),
    StructField("channel_title", StringType(), False),
    StructField("title", StringType(), False),
    StructField("img_url", StringType(), False),
    StructField("duration_seconds", IntegerType(), False),
    StructField("published_at", TimestampType(), False),
    StructField("view_count", IntegerType(), False),
    StructField("like_count", IntegerType(), True),
    StructField("created_at", TimestampType(), False)
])

final_df = spark.createDataFrame(processed_df.rdd, schema=schema)

# 결과를 S3에 저장 (출력 S3 경로 수정)
output_path = "s3://your-output-bucket/processed-data/"
final_df.write.mode("overwrite").parquet(output_path)

print("Data processing and storage completed!")
