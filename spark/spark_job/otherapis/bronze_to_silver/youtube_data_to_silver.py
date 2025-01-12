from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit, current_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
)
from custom_modules import s3_spark_module
import sys
import logging


logger = logging.getLogger(__name__)


# Spark 세션 생성
spark = SparkSession.builder.getOrCreate()

# 실행 시 전달받은 인자
args = sys.argv
source_path = args[1] + "/*.json"
target_path = args[2]
gender = args[3] if len(args) > 3 else None

# JSON 데이터 읽기
raw_df = spark.read.json(source_path)

# 모든 카테고리 추출 및 동적 처리
categories = raw_df.columns
exploded_df = None

for category in categories:
    category_df = raw_df.select(
        explode(col(category)).alias("video_data"), lit(category).alias("category_name")
    )
    exploded_df = category_df if exploded_df is None else exploded_df.union(category_df)

# 필요한 컬럼 추출 및 변환
processed_df = exploded_df.select(
    col("video_data.videoId").alias("video_id"),
    lit(gender).alias("gender"),
    col("category_name"),
    col("video_data.channelName").alias("channel_title"),
    col("video_data.title"),
    col("video_data.thumbnailUrl").alias("img_url"),
    col("video_data.duration").alias("duration_seconds"),
    col("video_data.publishedAt").cast(TimestampType()).alias("published_at"),
    col("video_data.viewCount").cast(IntegerType()).alias("view_count"),
    col("video_data.likeCount").cast(IntegerType()).alias("like_count"),
    current_timestamp().alias("created_at"),
)

# 스키마 정의
schema = StructType(
    [
        StructField("video_id", StringType(), False),
        StructField("gender", StringType(), True),
        StructField("category_name", StringType(), False),
        StructField("channel_title", StringType(), False),
        StructField("title", StringType(), False),
        StructField("img_url", StringType(), False),
        StructField("duration_seconds", IntegerType(), False),
        StructField("published_at", TimestampType(), False),
        StructField("view_count", IntegerType(), False),
        StructField("like_count", IntegerType(), True),
        StructField("created_at", TimestampType(), False),
    ]
)

# 스키마 적용
final_df = spark.createDataFrame(processed_df.rdd, schema=schema).repartition(1)

# 결과 저장
final_df.write.mode("overwrite").parquet(target_path)

logger.info(f"Data processed and saved to {target_path}")
