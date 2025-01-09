from pyspark.sql import SparkSession


def read_and_partition_s3_data(
    spark: SparkSession,
    s3_path: str,
    file_format: str,
    partition_column: str = None,
    target_partition_size_mb: int = 128,
    min_partitions: int = None,
    max_partitions: int = None,
):
    """
    Reads data from S3, optimizes partitioning, and returns a DataFrame.

    :param spark: SparkSession object
    :param s3_path: Path to the S3 bucket or prefix (e.g., "s3://bucket/path/")
    :param partition_column: Column name for partitioning (optional)
    :param target_partition_size_mb: Target size of each partition in MB (default: 128 MB)
    :param min_partitions: Minimum number of partitions (optional)
    :param max_partitions: Maximum number of partitions (optional)
    :return: Optimized Spark DataFrame
    """
    # Step 1: Read data from S3
    print(f"Reading data from: {s3_path}")
    if file_format == "parquet":
        print("Reading Parquet file...")
        df = spark.read.parquet(s3_path)
    elif file_format == "json":
        print("Reading JSON file...")
        df = spark.read.json(s3_path)
    elif file_format in ["csv", "txt"]:
        print("Reading CSV/TXT file...")
        df = spark.read.csv(s3_path, header=True, inferSchema=True)
    elif file_format == "html":
        print("Reading HTML file (requires external parsing)...")
        raise NotImplementedError(
            "HTML reading is not directly supported by Spark. Use external parsers like pandas."
        )
    else:
        raise ValueError(f"Unsupported file format: {file_format}")

    # Step 2: Calculate the number of partitions
    total_size_bytes = (
        spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        .getContentSummary(spark._jvm.org.apache.hadoop.fs.Path(s3_path))
        .getLength()
    )
    total_size_mb = total_size_bytes / (1024 * 1024)
    estimated_partitions = max(1, int(total_size_mb / target_partition_size_mb))

    if min_partitions:
        estimated_partitions = max(estimated_partitions, min_partitions)
    if max_partitions:
        estimated_partitions = min(estimated_partitions, max_partitions)

    print(f"Total data size: {total_size_mb:.2f} MB")
    print(f"Target partition size: {target_partition_size_mb} MB")
    print(f"Estimated partitions: {estimated_partitions}")

    # Step 3: Repartition data - 성능이 안나오면 coalesce 사용 가능.
    if partition_column:
        print(f"Repartitioning by column: {partition_column}")
        df = df.repartition(estimated_partitions, partition_column)
    else:
        print("Repartitioning without a specific column")
        df = df.repartition(estimated_partitions)

    return df
