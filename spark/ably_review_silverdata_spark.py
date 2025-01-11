from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date, array_join
from pyspark.conf import SparkConf
from datetime import datetime
import os
import glob

# 오늘 날짜
TODAY_DATE = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def create_spark_session():
    """
    SparkSession 생성 함수
    """
    conf = SparkConf()
    conf.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", 2)
    conf.set("spark.hadoop.fs.s3a.committer.magic.enabled", "true")
    conf.set("fs.s3a.committer.name", "magic")
    conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
    conf.set("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
    conf.set("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com")
    conf.set("spark.sql.parquet.compression.codec", "snappy")

    return SparkSession.builder.config(conf=conf).getOrCreate()

def get_filtered_input_files(input_path_pattern, exclude_file):
    """
    입력 경로에서 제외 파일을 제외한 JSON 파일 경로를 가져오는 함수

    :param input_path_pattern: S3 JSON 파일 패턴
    :param exclude_file: 제외할 파일 이름
    :return: 제외된 파일을 제외한 파일 경로 리스트
    """
    # S3 경로에서 파일 목록 가져오기
    all_files = glob.glob(input_path_pattern, recursive=True)
    filtered_files = [file for file in all_files if exclude_file not in file]
    return filtered_files

def transform_to_product_review_detail(spark, input_files, output_path, platform_name="ably"):
    """
    JSON 데이터를 스키마에 맞게 변환하고 Parquet로 저장

    :param spark: SparkSession
    :param input_files: 입력 JSON 파일 리스트
    :param output_path: 출력 Parquet 파일 경로
    :param platform_name: 플랫폼 이름 (기본값: "ably")
    """
    # JSON 데이터 로드
    df = spark.read.json(input_files)

    # 데이터 변환 및 매핑
    transformed_df = df.select(
        col("goods_sno").alias("product_id"),
        col("contents").alias("review_content"),
        col("eval").alias("review_rating"),
        to_date(col("created_at")).alias("review_date"),
        col("height").alias("reviewer_height"),
        col("weight").alias("reviewer_weight"),
        array_join(col("goods_option"), ", ").alias("selected_options")
    ).withColumn("created_at", lit(TODAY_DATE)) \
     .withColumn("platform", lit(platform_name))

    # Parquet 저장
    transformed_df.write.mode("overwrite").parquet(output_path)

def main():
    # SparkSession 생성
    spark = create_spark_session()

    # 입력 JSON 경로 패턴
    input_path_pattern = f"s3a://ablyrawdata/{TODAY_DATE}/Ably/ReviewData/**/*.json"

    # 제외할 파일 이름
    exclude_file = "goods_sno_list.json"

    # 출력 Parquet 경로
    output_parquet_path = f"s3a://ablyrawdata/silver/{TODAY_DATE}/Ably/ReviewData/Platform_product_review_detail_tb.parquet"

    # 제외 파일을 제외한 입력 파일 리스트
    input_files = get_filtered_input_files(input_path_pattern, exclude_file)

    # 데이터 변환 및 저장
    transform_to_product_review_detail(spark, input_files, output_parquet_path)

    print(f"Data successfully saved to {output_parquet_path}")

if __name__ == "__main__":
    main()