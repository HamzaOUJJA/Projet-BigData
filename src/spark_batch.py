import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

def create_spark_session():
    return SparkSession.builder \
        .appName("CSVToMinioBatch") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .getOrCreate()

def run_batch_job():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("sensor_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("average_speed", DoubleType(), True),
        StructField("traffic_density", DoubleType(), True)
    ])

    data_folder = "/opt/airflow/data"
    bucket = "traffic-batch-data"

    for file in os.listdir(data_folder):
        if file.endswith(".csv"):

            base_name = os.path.splitext(file)[0]
            file_path = os.path.join(data_folder, file)

            print(f"Processing: {file}")

            df = spark.read \
                .option("header", "true") \
                .schema(schema) \
                .csv(file_path)

            temp_path = f"s3a://{bucket}/temp_{base_name}/"
            final_path = f"s3a://{bucket}/{base_name}.parquet"

            # Force single parquet file
            df.coalesce(1).write.mode("overwrite").parquet(temp_path)

            # Hadoop FileSystem API
            hadoop_conf = spark._jsc.hadoopConfiguration()
            uri = spark._jvm.java.net.URI(temp_path)
            fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)

            temp_dir = spark._jvm.org.apache.hadoop.fs.Path(temp_path)
            final_file = spark._jvm.org.apache.hadoop.fs.Path(final_path)

            # Find part file
            for file_status in fs.listStatus(temp_dir):
                name = file_status.getPath().getName()
                if name.startswith("part-") and name.endswith(".parquet"):
                    fs.rename(file_status.getPath(), final_file)

            # Delete temp folder
            fs.delete(temp_dir, True)

            print(f"Created: {final_path}")

    spark.stop()

if __name__ == "__main__":
    run_batch_job()