from pyspark.sql import SparkSession
import time

if __name__ == "__main__":
    # SparkSession 생성
    spark = SparkSession.builder \
        .appName("Example PySpark Job") \
        .getOrCreate()

    print("PySpark job started!")
    
    # 간단한 작업 (예: 숫자 출력)
    data = spark.range(0, 100).toDF("number")
    data.show()

    # 실행 상태 확인을 위해 대기
    time.sleep(120)

    print("PySpark job completed!")
    spark.stop()
