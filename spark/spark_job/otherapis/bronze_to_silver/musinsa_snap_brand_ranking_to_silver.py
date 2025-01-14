from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    explode,
    count,
    current_date,
    row_number,
    lit,
    when,
    desc
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DateType,
)
from pyspark.sql.window import Window
import sys
import logging

logger = logging.getLogger(__name__)

# 스파크 세션 생성
spark = SparkSession.builder.getOrCreate()

# 실행 시 전달받은 인자
args = sys.argv
source_path = args[1] + "/*.json"
target_path = args[2]
logger.info(f"source_path: {source_path}")
logger.info(f"target_path: {target_path}")

# S3에서 모든 JSON 파일 읽기
raw_json_df = spark.read.json(source_path)

# JSON 처리 - 필요한 컬럼 추출 및 테이블 스키마에 맞게 변환
brands_df = (
    raw_json_df.selectExpr("data.list")
    .withColumn("list", explode(col("list")))
    .select("list.*")
)

table_df = brands_df.select(
    col("id").alias("brand_id"),
    col("nickname").alias("brand_name"),
    col("profileImageUrl").alias("img_url"),
    col("ranking.rank").alias("rank"),
    when(col("ranking.previousRank").isNull(), lit(0))  # NULL을 0으로 대체
    .otherwise(col("ranking.previousRank"))
    .cast("int")
    .alias("previous_rank"),
    col("followerCount").alias("follower_count"),
    col("snaps.labels").alias("labels"),
).withColumn("created_at", current_date())

# labels에서 name 필드 추출
category_names_df = table_df.withColumn("label", explode("labels")).select(
    col("brand_id"), col("label.name").alias("name")
)

# 브랜드별로 라벨 카운팅
category_names_with_counts_df = category_names_df.groupBy("brand_id", "name").count()

# 브랜드별 상위 2개의 라벨 선택
window_spec = Window.partitionBy("brand_id").orderBy(desc("count"))

top_labels_df = category_names_with_counts_df.withColumn(
    "rank", row_number().over(window_spec)
).filter(col("rank") <= 2)

# 각 브랜드별로 상위 2개의 라벨 추출
labels_split_df = top_labels_df.groupBy("brand_id").pivot("rank").agg(
    col("name").alias("label_name")
)

# 컬럼 이름을 명확히 수정
final_labels_df = labels_split_df.select(
    col("brand_id"),
    col("1").alias("labels_name_1"),  # 1위 라벨
    col("2").alias("labels_name_2")   # 2위 라벨
)

# 원본 데이터와 병합
final_table = table_df.join(
    final_labels_df, "brand_id", "left"
).select(
    "brand_id",
    "brand_name",
    "img_url",
    "rank",
    "previous_rank",
    "follower_count",
    "labels_name_1",
    "labels_name_2",
    "created_at",
)

# JSON 데이터 스키마 정의
schema = StructType(
    [
        StructField("brand_id", StringType(), True),
        StructField("brand_name", StringType(), True),
        StructField("img_url", StringType(), True),
        StructField("rank", IntegerType(), True),
        StructField("previous_rank", IntegerType(), True),
        StructField("follower_count", IntegerType(), True),
        StructField("labels_name_1", StringType(), True),
        StructField("labels_name_2", StringType(), True),
        StructField("created_at", DateType(), True),
    ]
)

# 스키마 적용
final_table_with_schema = spark.createDataFrame(
    final_table.rdd, schema=schema
).repartition(1)

# 결과를 S3 대상 경로로 저장
final_table_with_schema.write.mode("overwrite").parquet(target_path)

# 완료 메시지
logger.info(f"Processed JSON data has been saved to: {target_path}")
