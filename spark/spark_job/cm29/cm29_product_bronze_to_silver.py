import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    regexp_extract,
    input_file_name,
    when,
    concat_ws,
)
from datetime import datetime, timedelta
import sys
from functools import reduce
from cm29_product_detail_mapping_table import depth_mapping

# Logging 설정
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


# AWS 자격 증명 설정
BUCKET_NAME = "team3-2-s3"

# Spark 세션 생성
spark = SparkSession.builder.appName("29cm Silver Layer").getOrCreate()


today = (datetime.now() + timedelta(hours=9)).strftime("%Y-%m-%d")

# S3 경로
man_data_path = f"s3a://{BUCKET_NAME}/bronze/{today}/29cm/29cm_product/Man/*/*.json"

woman_data_path = f"s3a://{BUCKET_NAME}/bronze/{today}/29cm/29cm_product/Woman/*/*.json"

# 데이터 로드
try:
    man_data = (
        spark.read.option("multiline", "true")
        .json(man_data_path)
        .withColumn("gender", lit("남성"))
    )
    woman_data = (
        spark.read.option("multiline", "true")
        .json(woman_data_path)
        .withColumn("gender", lit("여성"))
    )
    logging.info("데이터 로드 성공")
except Exception as e:
    logging.error(f"데이터 로드 실패: {e}")
    exit(1)

# 남성/여성 데이터 결합
raw_data = man_data.union(woman_data)

# 파일 경로에서 4Depth 추출 (cat_depth_4 컬럼 생성)
raw_data = raw_data.withColumn("file_path", input_file_name())
raw_data = raw_data.withColumn(
    "cat_depth_4",
    regexp_extract(col("file_path"), r"/([^/]+)\.json$", 1),  # JSON 파일 이름에서 추출
)

# cat_depth_2, cat_depth_3, small_category_name 초기화
raw_data = raw_data.withColumn("cat_depth_2", lit(None).cast("string"))
raw_data = raw_data.withColumn("cat_depth_3", lit(None).cast("string"))
raw_data = raw_data.withColumn("small_category_name", lit(None).cast("string"))


# 매핑 로직 통합 처리
def apply_mapping(df, key, value):
    return (
        df.withColumn(
            "cat_depth_2",
            when(col("cat_depth_4") == key, lit(value[0])).otherwise(
                col("cat_depth_2")
            ),
        )
        .withColumn(
            "cat_depth_3",
            when(col("cat_depth_4") == key, lit(value[1])).otherwise(
                col("cat_depth_3")
            ),
        )
        .withColumn(
            "small_category_name",
            when(col("cat_depth_4") == key, lit(value[2])).otherwise(
                col("small_category_name")
            ),
        )
    )


# 매핑 테이블 적용
raw_data = reduce(
    lambda df, key_value: apply_mapping(df, *key_value), depth_mapping.items(), raw_data
)

# master_category_name 생성
raw_data = raw_data.withColumn(
    "master_category_name",
    concat_ws("-", col("gender"), col("cat_depth_2"), col("cat_depth_3")),
)

# master_category_tb 생성
master_category_df = raw_data.select(
    col("master_category_name"),
    col("gender").alias("cat_depth_1"),
    col("cat_depth_2"),
    col("cat_depth_3"),
).distinct()

# master_category_tb 저장
master_category_output_path = (
    f"s3a://{BUCKET_NAME}/silver/{today}/29cm/master_category_tb/"
)
master_category_df.write.mode("overwrite").parquet(master_category_output_path)
logging.info(f"master_category_tb 저장 완료: {master_category_output_path}")

# product_detail_tb 생성
product_detail_df = raw_data.select(
    col("platform"),
    col("master_category_name"),
    col("small_category_name"),  # 한글로 매핑된 small_category_name 포함
    col("product_id"),
    col("img_url"),
    col("product_name"),
    col("brand_name_kr"),
    col("brand_name_en"),
    col("original_price"),
    col("final_price"),
    col("discount_ratio"),
    col("review_counting"),
    col("review_avg_rating"),
    col("like_counting"),
    col("created_at"),
)

# product_detail_tb 저장
product_detail_output_path = (
    f"s3a://{BUCKET_NAME}/silver/{today}/29cm/product_detail_tb/"
)
product_detail_df.write.mode("overwrite").parquet(product_detail_output_path)
logging.info(f"product_detail_tb 저장 완료: {product_detail_output_path}")

# ranking_tb 생성
ranking_df = raw_data.select(
    col("platform"),
    col("master_category_name"),
    col("small_category_name"),  # 한글로 매핑된 small_category_name 포함
    col("product_id"),
    col("ranking"),
    col("created_at"),
)

# ranking_tb 저장
ranking_output_path = f"s3a://{BUCKET_NAME}/silver/{today}/29cm/ranking_tb/"
ranking_df.write.mode("overwrite").parquet(ranking_output_path)
logging.info(f"ranking_tb 저장 완료: {ranking_output_path}")

# sub_category_tb 생성
sub_category_df = raw_data.select(
    col("cat_depth_4").alias("sub_category_id"),
    col("platform"),
    col("master_category_name").alias("master_category_id"),
    col("small_category_name"),
    col("created_at"),
).distinct()

# sub_category_tb 저장
sub_category_output_path = f"s3a://{BUCKET_NAME}/silver/{today}/29cm/sub_category_tb/"
sub_category_df.write.mode("overwrite").parquet(sub_category_output_path)
logging.info(f"sub_category_tb 저장 완료: {sub_category_output_path}")

