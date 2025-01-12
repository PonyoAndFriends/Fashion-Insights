from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, lit, row_number
from pyspark.sql.window import Window
from datetime import datetime, timedelta
from ably_modules.ably_mapping_table import CATEGORY_PARAMS

# 오늘 날짜 - 날짜 path
TODAY_DATE = (datetime.now() + timedelta(hours=9)).strftime("%Y-%m-%d")


def create_spark_session():
    # SparkSession 생성
    spark = SparkSession.builder.getOrCreate()
    return spark


def making_ranking_table(
    spark,
    json_path,
    master_category_code,
    today_date,
    gender,
    category2depth,
    category3depth,
):
    # JSON 데이터 읽기
    df = spark.read.json(json_path)

    # 필요한 데이터 추출 및 컬럼 추가
    extracted_df = df.select(
        lit("ably").alias("platform"),
        lit(f"{gender}-{category2depth}-{category3depth}").alias("master_category_name"),
        col("logging.analytics.GOODS_SNO").cast("int").alias("product_id")
    )

    # ranking 컬럼 추가
    ranking_window = Window.orderBy(lit(1))
    ranked_df = extracted_df.withColumn("ranking", row_number().over(ranking_window))

    # created_at 컬럼 추가
    final_df = (
        ranked_df
        .withColumn("created_at", to_date(lit(today_date), "yyyy-MM-dd"))
        .select(
            "platform",
            "master_category_name",
            "product_id",
            "ranking",
            "created_at"
        )  # 컬럼 순서 보장
    )

    # Parquet 크기를 1개 파일로 병합
    final_df = final_df.coalesce(1)

    return final_df


def process_category(spark, category3depth, gender, category2depth, category4depth):
    try:
        # 공통 path
        file_name = f"{category3depth}/{gender}_{category2depth}_{category3depth}_{category4depth}"

        # input - filepath 조합
        input_path = f"s3a://team3-2-s3/bronze/{TODAY_DATE}/ably/ranking_data/*/*.json"

        # output - filepath 조합
        table_output_path = (
            f"s3a://team3-2-s3/silver/{TODAY_DATE}/ably/ranking_data/{file_name}.parquet"
        )

        # master_category_code 생성
        master_category_code = f"{gender}-{category2depth}-{category3depth}"
        print(f"Processing {master_category_code}-{category4depth}")

        # 데이터 처리
        cleaned_df = making_ranking_table(
            spark,
            input_path,
            master_category_code,
            TODAY_DATE,
            gender,
            category2depth,
            category3depth,
        )

        # 결과 저장
        cleaned_df.write.mode("overwrite").parquet(table_output_path)

    except Exception as e:
        print(f"Error processing category {category4depth}: {e}")


def main():
    spark = create_spark_session()

    for gender_dct in CATEGORY_PARAMS:
        # category1depth(성별) 추출
        gender = list(gender_dct["GENDER"].values())[0]

        # category2depth 추출
        for categorydepth in gender_dct["cat_2"]:
            category2depth = categorydepth["name"]  # name 값 추출

            # category3depth 추출
            for category3 in categorydepth["cat_3"]:
                category3depth = category3["name"]

                # category4depth 추출
                for category4 in category3["cat_4"]:
                    for _, category4depth in category4.items():
                        process_category(
                            spark,
                            category3depth,
                            gender,
                            category2depth,
                            category4depth,
                        )


if __name__ == "__main__":
    main()
