from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, explode, lit, row_number
from pyspark.sql.window import Window
from pyspark.conf import SparkConf
from datetime import datetime
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from ably_modules.ably_mapping_table import CATEGORY_PARAMS

# 오늘 날짜 - 날짜 path
TODAY_DATE = datetime.now().strftime('%Y-%m-%d')

def create_spark_session():
    # SparkConf 설정
    conf = SparkConf()
    conf.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", 2)
    conf.set("spark.hadoop.fs.s3a.committer.magic.enabled", "true")
    conf.set("fs.s3a.committer.name", "magic")
    conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
    conf.set("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
    conf.set("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com")
    conf.set(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    )
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.driver.memory", "2g")
    conf.set("spark.executor.cores", "2")
    conf.set("spark.dynamicAllocation.enabled", "true")
    conf.set("spark.dynamicAllocation.minExecutors", "2")
    conf.set("spark.dynamicAllocation.maxExecutors", "10")
    conf.set("spark.shuffle.service.enabled", "true")
    conf.set("spark.sql.shuffle.partitions", "100")
    conf.set("spark.sql.parquet.compression.codec", "snappy")

    # SparkSession 생성
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    return spark

def making_ranking_table(spark, json_path, master_category_code, today_date, gender, category2depth, category3depth):
    df = spark.read.json(json_path)

    # `logging.analytics.GOODS_SNO`와 기타 데이터를 추출
    extracted_df = df.select(
        col("item.logging.analytics.GOODS_SNO").alias("product_id")
    )

    # JSON 순서를 기반으로 `ranking` 추가
    ranking_window = Window.orderBy(lit(1))  # 고정값으로 정렬하여 순서 유지
    ranked_df = extracted_df.withColumn("ranking", row_number().over(ranking_window))

    # master_category_name 생성
    master_category_name = f"{gender}-{category2depth}-{category3depth}"
    final_df = ranked_df.withColumn("master_category_name", lit(master_category_name)) \
                        .withColumn("platform", lit("ably")) \
                        .withColumn("created_at", to_date(lit(today_date), "yyyy-MM-dd"))

    # Parquet 크기를 1개 파일로 병합
    final_df = final_df.coalesce(1)

    return final_df

def process_category(spark, category3depth, gender, category2depth, category4depth):
    try:
        # 공통 path
        file_name = f"{category3depth}/{gender}_{category2depth}_{category3depth}_{category4depth}"

        # input - filepath 조합
        input_path = f"s3a://ablyrawdata/{TODAY_DATE}/Ably/RankingData/*/*.json"

        # output - filepath 조합
        table_output_path = f"s3a://ablyrawdata/silver/{TODAY_DATE}/Ably/RankingData/{file_name}.parquet"

        master_category_code = f"{gender}-{category2depth}-{category3depth}"
        print(f"Processing {master_category_code}-{category4depth}")
        cleaned_df = making_ranking_table(
            spark, input_path, master_category_code, TODAY_DATE, gender, category2depth, category3depth
        )

        cleaned_df.write.mode("overwrite").parquet(table_output_path)

    except Exception as e:
        print(f"Error processing category {category4depth}: {e}")

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
                            print(f"    Category4 Depth Key: {cat_key}, Name: {category4depth}")

                            futures.append(
                                executor.submit(
                                    process_category,
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