from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    IntegerType,
    DateType,
)
from pyspark.sql.functions import col, from_json

from modules.musinsa_mappingtable import CATEGORY2DEPTH_MAPPING

import pendulum

LOCAL_TZ = pendulum.timezone("Asia/Seoul")

TODAY_DATE = pendulum.now(tz=LOCAL_TZ).to_date_string()


def create_spark_session():
    # SparkSession 생성
    spark = SparkSession.builder.getOrCreate()
    return spark


def etl_productdetail(spark, source_path, load_path):
    # JSON 스키마 정의
    schema = StructType(
        [
            StructField("platform", StringType(), True),
            StructField("master_category_name", StringType(), True),
            StructField("small_category_name", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("brand_name_kr", StringType(), True),
            StructField("brand_name_en", StringType(), True),
            StructField("original_price", StringType(), True),
            StructField("final_price", StringType(), True),
            StructField("discount_rate", StringType(), True),
            StructField("review_count", StringType(), True),
            StructField("review_avg_rating", StringType(), True),
            StructField("like_counting", IntegerType(), True),
            StructField("image_src", StringType(), True),
            StructField("created_at", DateType(), True),
        ]
    )

    raw_df = spark.read.text(source_path)

    # JSON 문자열을 파싱하여 구조화된 데이터로 변환
    parsed_df = raw_df.withColumn("json_data", from_json(col("value"), schema))

    # 필요한 열 선택
    final_df = parsed_df.select("json_data.*")

    # type casting
    final_df = final_df.withColumn("product_id", col("product_id").cast(IntegerType()))
    final_df = final_df.withColumn(
        "original_price", col("original_price").cast(IntegerType())
    )
    final_df = final_df.withColumn(
        "final_price", col("final_price").cast(IntegerType())
    )
    final_df = final_df.withColumn(
        "discount_rate", col("discount_rate").cast(IntegerType())
    )
    final_df = final_df.withColumn(
        "review_count", col("review_count").cast(IntegerType())
    )
    final_df = final_df.withColumn(
        "review_avg_rating", col("review_avg_rating").cast(FloatType())
    )
    final_df = final_df.coalesce(1)

    final_df.write.mode("overwrite").parquet(load_path)


def main():
    # Spark Session
    spark = create_spark_session()

    for category3depths in CATEGORY2DEPTH_MAPPING.values():
        for category3depth, category4depths in category3depths.items():
            for category4depth in category4depths:
                print(category3depth, category4depth)
                source_path = f"s3a://team3-2-s3/bronze/{TODAY_DATE}/musinsa/product_detail_data/{category3depth}/{category4depth}/*.json"
                load_path = f"s3a://team3-2-s3/silver/{TODAY_DATE}/musinsa/product_detail_data/{category3depth}/{category4depth}.parquet"
                etl_productdetail(spark, source_path, load_path)


if __name__ == "__main__":
    main()
