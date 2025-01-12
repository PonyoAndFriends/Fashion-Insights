from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
from datetime import datetime, timedelta
from ably_modules.ably_mapping_table import CATEGORY_PARAMS

# 오늘 날짜
TODAY_DATE = (datetime.now() + timedelta(hours=9)).strftime("%Y-%m-%d")


def create_spark_session():
    spark = SparkSession.builder.getOrCreate()
    return spark


def transform_data_to_product_detail(spark, json_path, category3depth, category4depth, today_date):
    df = spark.read.json(json_path)

    # 필요한 데이터 추출 및 매핑
    extracted_df = df.select(
        lit("ably").alias("platform"),
        lit(category3depth).alias("master_category_name"),
        lit(category4depth).alias("small_category_name"),
        col("item.logging.analytics.GOODS_SNO").alias("product_id"),
        col("item.image").alias("img_url"),
        col("item.logging.analytics.GOODS_NAME").alias("product_name"),
        col("item.logging.analytics.MARKET_NAME").alias("brand_name_kr"),
        col("item.first_page_rendering.original_price").alias("original_price"),
        col("item.logging.analytics.SALES_PRICE").alias("final_price"),
        col("item.logging.analytics.DISCOUNT_RATE").alias("discount_ratio"),
        col("item.logging.analytics.REVIEW_COUNT").alias("review_counting"),
        col("item.logging.analytics.REVIEW_RATING").alias("review_avg_rating"),
        col("item.logging.analytics.LIKES_COUNT").alias("like_counting"),
        lit(today_date).alias("created_at"),
    )

    # Parquet 크기를 1개 파일로 병합
    final_df = extracted_df.coalesce(1)
    return final_df


def process_product_details(row):
    try:
        # 공통 path
        file_name = f"{row['category3depth']}/{row['gender']}_{row['category2depth']}_{row['category3depth']}_{row['category4depth']}"

        # input - filepath 조합
        input_path = f"s3a://team3-2-s3/bronze/{TODAY_DATE}/ably/ranking_data/*/*.json"

        # output - filepath 조합
        table_output_path = f"s3a://team3-2-s3/silver/{TODAY_DATE}/ably/product_details/{file_name}.parquet"

        print(f"Processing product detail table for: {row['gender']}-{row['category2depth']}-{row['category3depth']}-{row['category4depth']}")

        # SparkSession을 드라이버에서 생성
        spark = create_spark_session()

        # 데이터 변환
        cleaned_df = transform_data_to_product_detail(
            spark, input_path, row['category3depth'], row['category4depth'], TODAY_DATE
        )

        # 데이터 저장
        cleaned_df.write.mode("overwrite").parquet(table_output_path)
    except Exception as e:
        print(f"Error processing product details for {row['category4depth']}: {e}")


def main():
    spark = create_spark_session()

    # CATEGORY_PARAMS 데이터를 Spark DataFrame으로 변환
    category_data = []
    for gender_dct in CATEGORY_PARAMS:
        gender = list(gender_dct["GENDER"].items())[0][1]

        for categorydepth in gender_dct["cat_2"]:
            category2depth = categorydepth["name"]

            for category3 in categorydepth["cat_3"]:
                category3depth = category3["name"]

                for category4 in category3["cat_4"]:
                    for _, category4depth in category4.items():
                        category_data.append((gender, category2depth, category3depth, category4depth))

    category_columns = ["gender", "category2depth", "category3depth", "category4depth"]
    category_df = spark.createDataFrame(category_data, category_columns)

    # DataFrame을 Pandas로 변환 후 작업 분산 처리
    rows = category_df.collect()  # 드라이버에서 데이터 수집
    for row in rows:
        process_product_details(row.asDict())  # 각 행을 처리


if __name__ == "__main__":
    main()
