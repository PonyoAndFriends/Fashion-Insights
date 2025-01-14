from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date, array_join
from datetime import datetime, timedelta

TODAY_DATE = datetime.now().strftime("%Y-%m-%d")

def create_spark_session():
    return SparkSession.builder.getOrCreate()

def transform_to_product_review_detail(spark, input_path, output_path):
    """
    JSON 데이터를 스키마에 맞게 변환하고 Parquet로 저장

    :param spark: SparkSession
    :param input_path: 입력 JSON 파일 경로 (와일드카드 포함)
    :param output_path: 출력 Parquet 파일 경로
    """
    # JSON 데이터 로드
    df = spark.read.json(input_path)

    # 데이터 변환 및 매핑
    # 스키마 변환 및 데이터 타입 캐스팅
    df_transformed = (
        df.select(
            col("goods_sno").cast("int").alias("product_id"),
            col("contents").cast("string").alias("review_content"),
            col("eval").cast("int").alias("review_rating"),
            to_date(col("created_at").cast("date"), "yyyy-MM-dd").alias("review_date"),
            col("height").cast("float").alias("reviewer_height"),
            col("weight").cast("float").alias("reviewer_weight"),
            array_join(col("goods_option"), ", ").alias("selected_options"),  # 리스트를 문자열로 변환
            to_date(lit(TODAY_DATE), "yyyy-MM-dd").alias("created_at")
        )
    )

    # 컬럼 순서를 보장
    final_df = df_transformed.select(
        "product_id",
        "review_content",
        "review_rating",
        "review_date",
        "reviewer_height",
        "reviewer_weight",
        "selected_options",
        "created_at",
    )

    # Parquet 저장
    final_df.write.mode("overwrite").parquet(output_path)

def main():
    spark = create_spark_session()

    # S3 경로 설정
    input_path = f"s3a://team3-2-s3/bronze/{TODAY_DATE}/ably/review_data/*/reviews_*.json"
    output_parquet_path = f"s3a://team3-2-s3/silver/{TODAY_DATE}/ably/review_data/platform_product_review_detail_tb.parquet"

    # 데이터 변환 및 저장
    try:
        transform_to_product_review_detail(spark, input_path, output_parquet_path)
        print(f"Data successfully saved to {output_parquet_path}")
    except Exception as e:
        print(f"Error processing data: {e}")

if __name__ == "__main__":
    main()
