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

    # 데이터 매핑 및 NULL 처리 보장
    extracted_df = df.select(
        lit("ably").alias("platform"),
        lit(category3depth).alias("master_category_name"),
        lit(category4depth).alias("small_category_name"),
        when(col("item.like.goods_sno").isNotNull(), col("item.like.goods_sno"))
        .otherwise(col("logging.analytics.GOODS_SNO")).alias("product_id"),  # 상품 ID
        col("item.image").alias("img_url"),  # 이미지 URL
        col("logging.analytics.GOODS_NAME").alias("product_name"),  # 상품 이름
        col("logging.analytics.MARKET_NAME").alias("brand_name_kr"),  # 브랜드 이름
        col("item.first_page_rendering.original_price").alias("original_price"),  # 원가
        col("logging.analytics.SALES_PRICE").alias("final_price"),  # 판매 가격
        col("logging.analytics.DISCOUNT_RATE").alias("discount_ratio"),  # 할인율
        col("logging.analytics.REVIEW_COUNT").alias("review_counting"),  # 리뷰 수
        col("logging.analytics.REVIEW_RATING").alias("review_avg_rating"),  # 리뷰 평균 점수
        col("logging.analytics.LIKES_COUNT").alias("like_counting"),  # 좋아요 수
        lit(today_date).alias("created_at")  # 수집 날짜
    )

    # 모든 행에 대해 NULL 허용 처리
    extracted_df = extracted_df.fillna({
        "product_id": None, "img_url": None, "product_name": None,
        "brand_name_kr": None, "original_price": None, "final_price": None,
        "discount_ratio": None, "review_counting": None, "review_avg_rating": None,
        "like_counting": None
    })

    return extracted_df

def process_product_details(row):
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

    # 데이터 저장
    print(f"Writing data to: {output_path}")
    cleaned_df.write.mode("overwrite").parquet(output_path)

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
