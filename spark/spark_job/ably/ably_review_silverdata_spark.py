import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date, array_join
from datetime import datetime, timedelta

TODAY_DATE = (datetime.now() + timedelta(hours=9)).strftime("%Y-%m-%d")

def create_spark_session():
    return SparkSession.builder.getOrCreate()

def get_s3_files(bucket_name, prefix, suffix=""):
    """
    S3에서 지정된 Prefix와 Suffix를 기준으로 파일 목록 가져오기

    :param bucket_name: S3 버킷 이름
    :param prefix: 파일 경로의 Prefix
    :param suffix: 파일 이름의 Suffix (예: ".json")
    :return: 파일 경로 리스트
    """
    s3_client = boto3.client("s3")
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    files = []
    while response:
        for content in response.get("Contents", []):
            key = content["Key"]
            if key.endswith(suffix):
                files.append(f"s3a://{bucket_name}/{key}")
        # 다음 페이지 처리
        if response.get("IsTruncated"):
            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix,
                ContinuationToken=response["NextContinuationToken"],
            )
        else:
            break
    return files

def transform_to_product_review_detail(spark, input_files, output_path):
    """
    JSON 데이터를 스키마에 맞게 변환하고 Parquet로 저장

    :param spark: SparkSession
    :param input_files: 입력 JSON 파일 리스트
    :param output_path: 출력 Parquet 파일 경로
    """
    # JSON 데이터 로드
    df = spark.read.json(input_files)

    # 데이터 변환 및 매핑
    transformed_df = (
        df.select(
            col("goods_sno").alias("product_id"),
            col("contents").alias("review_content"),
            col("eval").alias("review_rating"),
            to_date(col("created_at")).alias("review_date"),
            col("height").cast("float").alias("reviewer_height"),
            col("weight").cast("float").alias("reviewer_weight"),
            array_join(col("goods_option"), ", ").alias("selected_options"),
        )
        .withColumn("created_at", to_date(lit(TODAY_DATE), "yyyy-MM-dd"))
    )

    # 컬럼 순서를 보장
    final_df = transformed_df.select(
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
    bucket_name = "team3-2-s3"
    prefix = f"bronze/{TODAY_DATE}/ably/review_data/"
    suffix = "reviews_*.json"

    # 파일 경로 가져오기
    input_files = get_s3_files(bucket_name, prefix, suffix)

    if not input_files:
        print("No input files found. Exiting.")
        return

    print(f"Input Files: {input_files}")

    output_parquet_path = f"s3a://{bucket_name}/silver/{TODAY_DATE}/ably/review_data/platform_product_review_detail_tb.parquet"

    # 데이터 변환 및 저장
    transform_to_product_review_detail(spark, input_files, output_parquet_path)
    print(f"Data successfully saved to {output_parquet_path}")

if __name__ == "__main__":
    main()
