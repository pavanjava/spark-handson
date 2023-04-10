from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "orders"

spark = SparkSession.builder.appName("sparkdev-kafka-integration").master("local[*]").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("ordertime", LongType(), True),
    StructField("orderid", IntegerType(), True),
    StructField("itemid", StringType(), True),
    StructField("amount", FloatType(), True)
])

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .load()

stream_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

structured_df = stream_df.withColumn("value", from_json("value", schema=schema)) \
    .select(col("key"), col("value.*"))

structured_df.filter(col("amount") > 500).writeStream \
    .format("console") \
    .outputMode("update") \
    .start() \
    .awaitTermination()
