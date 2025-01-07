from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    explode,
    collect_list,
    struct,
    row_number,
    to_json,
)
from pyspark.sql.window import Window
from custom_modules import spark_optimize, s3_spark_module
import sys

# 스파크 세션 생성
spark = SparkSession.builder.appName("Transform JSON to Table").getOrCreate()

# 실행 시 전달받은 인자
args = sys.argv
source_path = args[1]
target_path = args[2]

# 1. S3에서 모든 JSON 파일 읽기
all_json_df = s3_spark_module.read_and_partition_s3_data(spark, source_path, "json")

# 2. JSON 처리
# 필요한 컬럼 추출 및 테이블 스키마에 맞게 변환
brands_df = (
    all_json_df.selectExpr("data.list")
    .withColumn("list", explode(col("list")))
    .select("list.*")
)
optimized_partitions = spark_optimize.calculate_final_partition_num(spark, brands_df)
brands_df = brands_df.coalesce(
    optimized_partitions
)  # 데이터 균등 분배 원하면 repartition

table_df = brands_df.select(
    col("id").alias("brand_id"),
    col("nickname").alias("brand_nickname"),
    col("contentType").alias("content_type"),
    col("ranking.rank").alias("rank"),
    col("ranking.previousRank").alias("previous_rank"),
    col("followerCount").alias("follower_count"),
    col("labels"),
    col("createdAt").alias("created_at"),
)


# Labels 데이터 처리: categoryName 별로 name 빈도수 높은 2개 가져오기
labels_df = table_df.withColumn("label", explode("labels")).select(
    col("brand_id"),
    col("label.name").alias("name"),
    col("label.categoryName").alias("category_name"),
)

window = Window.partitionBy("brand_id", "category_name").orderBy(col("name").desc())
labels_ranked = (
    labels_df.groupBy("brand_id", "category_name", "name")
    .count()
    .withColumn("rank", row_number().over(window))
    .filter(col("rank") <= 2)
)

brand_colors_df = labels_ranked.groupBy("brand_id").agg(
    to_json(
        collect_list(struct(col("category_name").alias("categoryName"), col("name")))
    ).alias("brand_color")
)

final_table = table_df.join(brand_colors_df, "brand_id", "left").select(
    "brand_id",
    "content_type",
    "rank",
    "previous_rank",
    "follower_count",
    "brand_color",
    "created_at",
)

# 3. 결과를 S3 대상 경로로 저장
final_table.write.mode("overwrite").parquet(target_path)

# 4. 완료 메시지
print(f"Processed JSON data has been saved to: {target_path}")
