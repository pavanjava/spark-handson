from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("spark_streaming_dev").master("local[*]").getOrCreate()

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "stocks"

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("ticker", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("feed_ts", TimestampType(), True)
])

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .load()

stream_df = df.selectExpr("CAST(value AS STRING)")

stream_df = stream_df.withColumn("value", from_json("value", schema=schema)).select(col("value.*"))

window_setup = window(
    col("feed_ts"),
    windowDuration="10 seconds",
    slideDuration="10 seconds",
).alias("window")

stream_df = stream_df.groupBy(col("ticker"), window_setup).count()

stream = stream_df.writeStream \
    .format("console") \
    .option("truncate", False)\
    .option("compression", "none")\
    .outputMode("complete") \
    .start()

stream.awaitTermination()