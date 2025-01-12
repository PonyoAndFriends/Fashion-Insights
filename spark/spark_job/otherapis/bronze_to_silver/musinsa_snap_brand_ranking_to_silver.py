from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    explode,
    collect_list,
    current_timestamp,
    count,
    desc,
    row_number
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType,
    TimestampType,
)
from pyspark.sql.window import Window
from custom_modules import s3_spark_module
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
    col("ranking.previousRank").alias("previous_rank"),
    col("followerCount").alias("follower_count"),
    col("snaps.labels").alias("labels"),
).withColumn("created_at", current_timestamp())

# Labels 데이터 처리: name 필드만 추출하고 등장 횟수를 카운팅
labels_df = table_df.withColumn("label", explode("labels")).select(
    col("brand_id"), col("label.name").alias("name")
)

# brand_id별 name 등장 횟수 계산
label_counts_df = labels_df.groupBy("brand_id", "name").agg(
    count("name").alias("count")
)

# 상위 2개의 name 선택 (윈도우 함수 사용)
window_spec = Window.partitionBy("brand_id").orderBy(desc("count"))
top_labels_df = label_counts_df.withColumn("rank", row_number().over(window_spec))
filtered_labels_df = top_labels_df.filter(col("rank") <= 2).groupBy("brand_id").agg(
    collect_list("name").alias("top_label_names")
)

# 원본 테이블과 결합
table_with_top_labels = table_df.join(filtered_labels_df, "brand_id", "left").select(
    "brand_id",
    "brand_name",
    "img_url",
    "rank",
    "previous_rank",
    "follower_count",
    "top_label_names",
    "created_at",
)

# JSON 데이터 스키마 정의
schema = StructType(
    [
        StructField("brand_id", StringType(), False),
        StructField("brand_name", StringType(), False),
        StructField("img_url", StringType(), False),
        StructField("rank", IntegerType(), False),
        StructField("previous_rank", IntegerType(), False),
        StructField("follower_count", IntegerType(), False),
        StructField("top_label_names", ArrayType(StringType()), False),
        StructField("created_at", TimestampType(), False),
    ]
)

# 스키마 적용
# final_table_with_schema = spark.createDataFrame(
#     table_with_top_labels.rdd, schema=schema
# ).repartition(1)

final_table_with_schema = table_df.repartition(1)

# 결과를 S3 대상 경로로 저장
final_table_with_schema.write.mode("overwrite").parquet(target_path)

# 완료 메시지
logger.info(f"Processed JSON data has been saved to: {target_path}")
