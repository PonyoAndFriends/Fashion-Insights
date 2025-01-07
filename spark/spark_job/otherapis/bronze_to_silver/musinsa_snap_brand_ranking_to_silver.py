from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    explode,
    collect_list,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType,
    TimestampType,
)
from custom_modules import s3_spark_module
import sys, logging

logger = logging.getLogger(__name__)

# 스파크 세션 생성
spark = SparkSession.builder.appName(
    "musinsa_snap_brand_ranking_to_silver_s3"
).getOrCreate()

# 실행 시 전달받은 인자
args = sys.argv
source_path = args[1]
target_path = args[2]

# S3에서 모든 JSON 파일 읽기
raw_json_df = s3_spark_module.read_and_partition_s3_data(spark, source_path, "json")

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
    col("contentType").alias("content_type"),
    col("ranking.rank").alias("rank"),
    col("ranking.previousRank").alias("previous_rank"),
    col("followerCount").alias("follower_count"),
    col("labels"),
    col("createdAt").alias("created_at"),
)

# Labels 데이터 처리: name 필드만 리스트로 묶어서 저장
labels_df = table_df.withColumn("label", explode("labels")).select(
    col("brand_id"), col("label.name").alias("name")
)

# brand_id별 name 리스트 생성
label_names_df = labels_df.groupBy("brand_id").agg(
    collect_list("name").alias("label_names")
)

# 원본 테이블과 결합
final_table_with_labels = table_df.join(label_names_df, "brand_id", "left").select(
    "brand_id",
    "brand_name",
    "img_url",
    "content_type",
    "rank",
    "previous_rank",
    "follower_count",
    "label_names",
    "created_at",
)

# JSON 데이터 스키마 정의
schema = StructType(
    [
        StructField("brand_id", StringType(), False),
        StructField("brand_name", StringType(), False),
        StructField("img_url", StringType(), False),
        StructField("content_type", StringType(), False),
        StructField("rank", IntegerType(), False),
        StructField("previous_rank", IntegerType(), False),
        StructField("follwer_count", IntegerType(), False),
        StructField("label_names", ArrayType(StringType()), False),
        StructField("created_at", TimestampType(), True),
    ]
)

# 스키마 적용
final_table_with_schema = spark.createDataFrame(
    final_table_with_labels.rdd, schema=schema
)


# 결과를 S3 대상 경로로 저장
final_table_with_schema.write.mode("overwrite").parquet(target_path)

# 완료 메시지
logger.info(f"Processed JSON data has been saved to: {target_path}")
