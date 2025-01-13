from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit, to_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DateType,
)
from datetime import datetime, timedelta
import sys
import logging


logger = logging.getLogger(__name__)


# Spark 세션 생성
spark = SparkSession.builder.getOrCreate()

# 실행 시 전달받은 인자
args = sys.argv
source_path = args[1] + "/*/*.json"
target_path = args[2]
gender = args[3] if len(args) > 3 else None

# 오늘 날짜 (데이터 수집 날짜)
today_date = (datetime.now() + timedelta(hours=9)).strftime("%Y-%m-%d")

# JSON 데이터 읽기
raw_df = spark.read.json(source_path)

# 데이터 변환
transformed_df = raw_df.select(
    col("videoId").alias("video_id"),  # YouTube 영상 고유 ID
    lit(gender).cast("string").alias("gender"),  # 성별 (기본값 NULL)
    col("category_name").alias("category_name"),  # 카테고리 이름
    col("channelName").alias("channel_title"),  # 채널 이름
    col("title").alias("title"),  # 영상 제목
    col("thumbnailUrl").alias("img_url"),  # 썸네일 이미지 URL
    col("duration").cast("int").alias("duration_seconds"),  # 영상 길이 (초 단위)
    to_timestamp(col("publishedAt"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("published_at"),  # 게시 날짜와 시간
    col("viewCount").cast("int").alias("view_count"),  # 조회수
    col("likeCount").cast("int").alias("like_count"),  # 좋아요 수
    lit(today_date).cast("date").alias("created_at")  # 데이터 수집 날짜
)

# 스키마 정의
schema = StructType(
    [
        StructField("video_id", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("category_name", StringType(), False),
        StructField("channel_title", StringType(), False),
        StructField("title", StringType(), False),
        StructField("img_url", StringType(), False),
        StructField("duration_seconds", IntegerType(), False),
        StructField("published_at", DateType(), False),
        StructField("view_count", IntegerType(), False),
        StructField("like_count", IntegerType(), False),
        StructField("created_at", DateType(), False),
    ]
)

# 스키마 적용
final_df = transformed_df.repartition(1)

# 결과 저장
final_df.write.mode("overwrite").parquet(target_path)

logger.info(f"Data processed and saved to {target_path}")
