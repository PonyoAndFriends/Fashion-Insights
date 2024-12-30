from pyspark.sql import SparkSession
import time

if __name__ == "__main__":
    # SparkSession 생성
    spark = SparkSession.builder.appName("SparkSleepJob").getOrCreate()

    print("Spark job started. Sleeping for 2 minutes...")
    time.sleep(120)  # 2분 대기
    print("Spark job completed.")

    spark.stop()
