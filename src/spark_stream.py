from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

def create_spark_session():
    return SparkSession.builder \
        .appName("KafkaToMinioStreaming") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4") \
        .getOrCreate()



def start_streaming():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Schema definition
    schema = StructType([
        StructField("sensor_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("average_speed", DoubleType(), True),
        StructField("traffic_density", DoubleType(), True)
    ])

    # 1. Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", "traffic-stream") \
        .option("startingOffsets", "earliest") \
        .load()

    # 2. Extract JSON
    parsed_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # 3. Sink to Minio (Parquet)
    # Using availableNow=True so the Airflow task can finish
    query = parsed_df.writeStream \
        .format("parquet") \
        .option("path", "s3a://traffic-streaming-data/raw_traffic/") \
        .option("checkpointLocation", "s3a://traffic-streaming-data/checkpoints/") \
        .outputMode("append") \
        .trigger(processingTime='10 seconds') \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    start_streaming()