from pyspark.conf import SparkConf

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from pyspark.sql.functions import col, from_json, explode, to_date, to_timestamp, lit
from pyspark.sql.types import *

from modules.musinsa_mappingtable import CATEGORY2DEPTH_MAPPING

import pendulum

SCHEMA = StructType([
    StructField("list", ArrayType(StructType([
        StructField("goods", StructType([
            StructField("goodsNo", IntegerType(), True),
        ]), True),
        StructField("content", StringType(), True),
        StructField("grade", StringType(), True),
        StructField("createDate", StringType(), True),
        StructField("userProfileInfo", StructType([
            StructField("userHeight", IntegerType(), True),
            StructField("userWeight", IntegerType(), True),
        ]), True),
        StructField("goodsOption", StringType(), True),
    ])), True)
])

LOCAL_TZ = pendulum.timezone("Asia/Seoul")

TODAY_DATE = pendulum.now(tz=LOCAL_TZ).to_date_string()

def create_spark_session():
    # SparkConf 설정
    conf = SparkConf()
    conf.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", 2)
    conf.set("spark.hadoop.fs.s3a.committer.magic.enabled", "true")
    conf.set("fs.s3a.committer.name", "magic")
    conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
    conf.set("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
    conf.set("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com")  # 리전 엔드포인트 수정
    conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.driver.memory", "2g")
    conf.set("spark.sql.parquet.compression.codec", "snappy")  # Parquet 파일 압축 설정
    # SparkSession 생성
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    return spark

def etl_productreview(spark, source_path, load_path):
    raw_df = spark.read.text(source_path)
                
    parsed_df = raw_df.withColumn("parsed", from_json(col("value"), SCHEMA))
                
    exploded_df = parsed_df.select(explode(col("parsed.list")).alias("review"))
                
    final_df = exploded_df.select(
        col("review.goods.goodsNo").alias("product_id"),
        col("review.content").alias("review_content"),
        col("review.grade").alias("review_rating"),
        col("review.createDate").alias("review_date"),
        col("review.userProfileInfo.userHeight").alias("reviewer_height"),
        col("review.userProfileInfo.userWeight").alias("reviewer_weight"),
        col("review.goodsOption").alias("selected_options")
    )
                
    final_df = final_df.withColumn("review_date", to_date(to_timestamp(col("review_date"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")))
    final_df = final_df.withColumn("create_at", to_date(lit(TODAY_DATE), "yyyy-MM-dd")) 
                
    final_df = final_df.distinct()
    final_df = final_df.coalesce(1)
                
    final_df.write.mode("overwrite").parquet(load_path)
    
def main():
    # Spark Session
    spark = create_spark_session()
    
    for category3depths in CATEGORY2DEPTH_MAPPING.values():
        for category3depth, category4depths in category3depths.items():
            for category4depth in category4depths:
                print(category3depth, category4depth)
                source_path = f"s3a://project4-raw-data/{TODAY_DATE}/Musinsa/ProductReviewData/{category3depth}/{category4depth}/*.json"
                load_path = f"s3a://project4-silver-data/{TODAY_DATE}/Musinsa/ProductReviewData/{category3depth}/{category4depth}.parquet"
                etl_productreview(spark, source_path, load_path)
                

if __name__ == "__main__":
    main()
