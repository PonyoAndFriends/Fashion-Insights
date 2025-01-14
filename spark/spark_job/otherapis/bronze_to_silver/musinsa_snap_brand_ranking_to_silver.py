from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    explode,
    collect_list,
    current_date,
    row_number,
    to_json,
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
# 각 브랜드의 모든 snap에 대해 labels를 펼쳐서 처리
category_names_df = table_df.withColumn("label", explode("labels")).select(
    col("brand_id"), col("label.name").alias("name")
)

# 브랜드별로 라벨 카운팅
# 각 brand_id에 대해 label name의 빈도를 계산
category_names_with_counts_df = category_names_df.groupBy("brand_id", "name").count()

# 브랜드별 상위 3개의 라벨 선택
# 라벨 빈도를 기준으로 내림차순 정렬 후 상위 3개 추출
window_spec = Window.partitionBy("brand_id").orderBy(desc("count"))

top_3_labels_df = category_names_with_counts_df.withColumn(
    "rank", row_number().over(window_spec)
).filter(col("rank") <= 3)

# 상위 3개의 라벨을 리스트로 그룹화
# 그룹화된 라벨 리스트를 JSON 형식으로 변환

top_3_labels_grouped_df = top_3_labels_df.groupBy("brand_id").agg(
    collect_list("name").alias("top_labels")
).withColumn(
    "label_names", to_json(col("top_labels"))
)

# 최종 데이터 생성
# 원본 데이터와 상위 3개의 라벨 데이터를 병합
final_table = table_df.join(
    top_3_labels_grouped_df, "brand_id", "left"
).select(
    "brand_id",
    "brand_name",
    "img_url",
    "rank",
    "previous_rank",
    "follower_count",
    "label_names",
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
        StructField("label_names", StringType(), True),
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
