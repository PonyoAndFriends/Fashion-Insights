from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, when
from datetime import datetime, timedelta
from ably_modules.ably_mapping_table import CATEGORY_PARAMS

# 오늘 날짜
TODAY_DATE = (datetime.now() + timedelta(hours=9)).strftime("%Y-%m-%d")

def create_spark_session():
    spark = SparkSession.builder.getOrCreate()
    return spark

def transform_data_to_product_detail(spark, json_path, category3depth, category4depth, today_date):
    # JSON 읽기 (PERMISSIVE 모드)
    df = spark.read.option("mode", "PERMISSIVE") \
        .option("columnNameOfCorruptRecord", "_corrupt_record") \
        .json(json_path)

    # 필요한 컬럼들
    columns = df.columns
    
    required_columns = {
        "platform": lit("ably"),
        "master_category_name": lit(category3depth),
        "small_category_name": lit(category4depth),
        "product_id": when(col("item.like.goods_sno").isNotNull(), col("item.like.goods_sno"))
        .otherwise(col("logging.analytics.GOODS_SNO")) if "item.like.goods_sno" in columns else lit(None),
        "img_url": col("item.image") if "item.image" in columns else lit(None),
        "product_name": col("logging.analytics.GOODS_NAME") if "logging.analytics.GOODS_NAME" in columns else lit(None),
        "brand_name_kr": col("logging.analytics.MARKET_NAME") if "logging.analytics.MARKET_NAME" in columns else lit(None),
        "original_price": col("item.first_page_rendering.original_price") if "item.first_page_rendering.original_price" in columns else lit(None),
        "final_price": col("logging.analytics.SALES_PRICE") if "logging.analytics.SALES_PRICE" in columns else lit(None),
        "discount_ratio": col("logging.analytics.DISCOUNT_RATE") if "logging.analytics.DISCOUNT_RATE" in columns else lit(None),
        "review_counting": col("logging.analytics.REVIEW_COUNT") if "logging.analytics.REVIEW_COUNT" in columns else lit(None),
        "review_avg_rating": col("logging.analytics.REVIEW_RATING") if "logging.analytics.REVIEW_RATING" in columns else lit(None),
        "like_counting": col("logging.analytics.LIKES_COUNT") if "logging.analytics.LIKES_COUNT" in columns else lit(None),
        "created_at": lit(today_date)
    }

    # 데이터 매핑 및 NULL 처리
    extracted_df = df.select(*[required_columns[col_name].alias(col_name) for col_name in required_columns])

    return extracted_df

def process_product_details(row):
    try:
        # 경로 설정
        file_name = f"{row['category3depth']}/{row['gender']}_{row['category2depth']}_{row['category3depth']}_{row['category4depth']}"
        input_path = f"s3a://team3-2-s3/bronze/{TODAY_DATE}/ably/ranking_data/*/*.json"
        output_path = f"s3a://team3-2-s3/silver/{TODAY_DATE}/ably/product_details/{file_name}.parquet"

        print(f"Processing product detail for: {row['category4depth']}")

        # SparkSession 생성 및 데이터 변환
        spark = create_spark_session()
        cleaned_df = transform_data_to_product_detail(
            spark, input_path, row['category3depth'], row['category4depth'], TODAY_DATE
        )

        # 데이터가 있을 경우에만 저장
        if cleaned_df.limit(1).count() > 0:
            print(f"Writing data to: {output_path}")
            cleaned_df.write.mode("overwrite").parquet(output_path)
        else:
            print(f"No data found for: {row['category4depth']}. Skipping file creation.")

    except Exception as e:
        print(f"Error processing row {row['category4depth']}: {e}")  # 예외 로그 출력

def main():
    spark = create_spark_session()

    # CATEGORY_PARAMS -> DataFrame 변환
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

    # 각 행에 대해 데이터 처리
    rows = category_df.collect()
    for row in rows:
        process_product_details(row.asDict())

if __name__ == "__main__":
    main()
