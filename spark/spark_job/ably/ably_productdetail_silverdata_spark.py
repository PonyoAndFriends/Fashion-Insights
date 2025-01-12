from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, lit
from pyspark.conf import SparkConf
from datetime import datetime
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from ably_modules.ably_mapping_table import CATEGORY_PARAMS

# 오늘 날짜
TODAY_DATE = datetime.now().strftime("%Y-%m-%d")


def create_spark_session():
    # SparkSession 생성
    spark = SparkSession.builder.getOrCreate()
    return spark


def transform_data_to_product_detail(
    spark, json_path, master_category_name, today_date
):
    df = spark.read.json(json_path)

    # 필요한 데이터 추출 및 매핑
    extracted_df = df.select(
        col("item.logging.analytics.GOODS_SNO").alias("product_id"),
        col("item.logging.analytics.goods_name").alias("product_name"),
        col("item.logging.analytics.MARKET_NAME").alias("brand_name_kr"),
        col("item.logging.analytics.discount_rate").alias("discount_ratio"),
        col("item.logging.analytics.price").alias("final_price"),
        col("item.logging.analytics.ORIGINAL_PRICE").alias("original_price"),
        col("item.logging.analytics.REVIEW_COUNT").alias("review_counting"),
        col("item.logging.analytics.REVIEW_RATING").alias("review_avg_rating"),
        col("item.logging.analytics.LIKES_COUNT").alias("like_counting"),
        col("item.logging.analytics.CATEGORY_NAME").alias("small_category_name"),
        col("item.logging.analytics.IMAGE_URL").alias("img_url"),
    )

    # 추가 컬럼 생성
    final_df = (
        extracted_df.withColumn("platform", lit("ably"))
        .withColumn("master_category_name", lit(master_category_name))
        .withColumn("brand_name_en", lit(None))
        .withColumn("created_at", to_date(lit(today_date), "yyyy-MM-dd"))
    )

    # Parquet 크기를 1개 파일로 병합
    final_df = final_df.coalesce(1)

    return final_df


def process_product_details(
    spark, category3depth, gender, category2depth, category4depth
):
    try:
        # 공통 path
        file_name = f"{category3depth}/{gender}_{category2depth}_{category3depth}_{category4depth}"

        # input - filepath 조합
        input_path = f"s3a://ablyrawdata/{TODAY_DATE}/Ably/RankingData/*/*.json"

        # output - filepath 조합
        table_output_path = f"s3a://silver/{TODAY_DATE}/ably/product_details/{file_name}.parquet"

        master_category_name = f"{gender}-{category2depth}-{category3depth}"
        print(
            f"Processing product detail table for: {master_category_name}-{category4depth}"
        )
        cleaned_df = transform_data_to_product_detail(
            spark, input_path, master_category_name, TODAY_DATE
        )

        cleaned_df.write.mode("overwrite").parquet(table_output_path)

    except Exception as e:
        print(f"Error processing product details for {category4depth}: {e}")


def main():
    spark = create_spark_session()

    with ThreadPoolExecutor(max_workers=10) as executor:  # 최대 10개의 스레드 사용
        futures = []

        for gender_dct in CATEGORY_PARAMS:
            # category1depth(성별) 추출
            gender = list(gender_dct["GENDER"].items())[0][1]

            # category2depth 추출
            for categorydepth in gender_dct["cat_2"]:
                category2depth = categorydepth["name"]  # name 값 추출 (cat_2의 name)
                print(f"Category2 Depth Name: {category2depth}")

                # category3depth 추출
                for category3 in categorydepth["cat_3"]:
                    category3depth = category3["name"]  # name 값 추출 (cat_3의 name)
                    print(f"  Category3 Depth Name: {category3depth}")

                    # category4depth 추출
                    for category4 in category3["cat_4"]:
                        for cat_key, category4depth in category4.items():
                            print(
                                f"    Category4 Depth Key: {cat_key}, Name: {category4depth}"
                            )

                            futures.append(
                                executor.submit(
                                    process_product_details,
                                    spark,
                                    category3depth,
                                    gender,
                                    category2depth,
                                    category4depth,
                                )
                            )

        for future in as_completed(futures):
            try:
                future.result()  # 작업 완료 대기
            except Exception as e:
                print(f"Error in thread execution: {e}")


if __name__ == "__main__":
    main()
