from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.sql.functions import col, explode, lit
from datetime import datetime, timedelta

# SparkSession 생성
spark = SparkSession.builder.getOrCreate()

# 오늘 날짜
today_date = datetime.now()
yesterday_date = today_date
yesterday_date_str = yesterday_date.strftime("%Y-%m-%d")

categorycodelist = ["103000", "002000", "001000"]
gflist = ['A', 'M', 'F']
ageBandlist = ["AGE_BAND_ALL", "AGE_BAND_MINOR", "AGE_BAND_20", "AGE_BAND_25", "AGE_BAND_30", "AGE_BAND_35", "AGE_BAND_40"]

schema = StructType([
    StructField("id", StringType(), True),
    StructField("brandName", StringType(), True),
    StructField("productName", StringType(), True),
    StructField("discountRatio", FloatType(), True),
    StructField("finalPrice", FloatType(), True),
    StructField("ranking", IntegerType(), True)
])

middle_schema = StructType([
    StructField("id", StringType(), True),
    StructField("brandName", StringType(), True),
    StructField("productName", StringType(), True),
    StructField("discountRatio", FloatType(), True),
    StructField("finalPrice", FloatType(), True),
    StructField("ranking", IntegerType(), True),
    StructField("categoryCode", StringType(), True),
    StructField("gf", StringType(), True),
    StructField("ageBand", StringType(), True),
    StructField("platform", StringType(), True),
    StructField("date", StringType(), True)
])

final_df = spark.createDataFrame([], schema)

def making_category_table(df, categorycode, gf, ageband):
    small_df = spark.createDataFrame([], schema)
    for i in range(1, 18):
        items_df = (
            df.select(explode(col("data.modules")[i]["items"]).alias("item"))
            .select(
                col("item.id").alias("id"),
                col("item.info.brandName").alias("brandName"),
                col("item.info.productName").alias("productName"),
                col("item.info.discountRatio").alias("discountRatio"),
                col("item.info.finalPrice").alias("finalPrice"),
                col("item.info.onClickBrandName.eventLog.ga4.payload.index").alias("ranking")
            )
        )
        small_df = small_df.union(items_df)

    small_df = small_df.withColumn("categoryCode", lit(categorycode))
    small_df = small_df.withColumn("gf", lit(gf))
    small_df = small_df.withColumn("ageBand", lit(ageband))
    small_df = small_df.withColumn("platform", lit("Musinsa"))
    small_df = small_df.withColumn("date", lit(yesterday_date_str))
    df_cleaned = small_df.dropna()
    return df_cleaned

for categorycode in categorycodelist:
    middle_df = spark.createDataFrame([], middle_schema)
    for gf in gflist:
        for ageBand in ageBandlist:
            print("Processing", categorycode, gf, ageBand)
            path = f"s3a://source-bucket-hs/{yesterday_date_str}/Musinsa/RankingData/{categorycode}/musinsa_{gf}_{ageBand}_{categorycode}.json"
            df = spark.read.json(path)
            df_cleaned = making_category_table(df, categorycode, gf, ageBand)
            middle_df = middle_df.union(df_cleaned)
    middle_df = middle_df.repartition(1)
    save_path = f"s3a://destination-bucket-hs/{yesterday_date_str}/Musinsa/RankingData/{categorycode}.parquet"
    middle_df.write.mode("overwrite").parquet(save_path)
