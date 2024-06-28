from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "orders"

spark = SparkSession.builder.appName("sparkdev-kafka-integration").master("local[*]").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("tag", StringType(), True),
    StructField("name", StringType(), True),
    StructField("items", StructType([
        StructField("product1", IntegerType(), True),
        StructField("product2", IntegerType(), True),
    ]), True)
])

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .load()

# df = spark.readStream.format("kafka") \
#     .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
#     .option("subscribe", KAFKA_TOPIC) \
#     .option("kafka.security.protocol", "SASL_SSL") \
#     .option("kafka.sasl.mechanism", "PLAIN") \
#     .option("kafka.sasl.jaas.config", """kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="DH6J3NSQDVCJFVYE" password="2QRFiZSPcPDPZhoOioicjPBgYsnVlkskayCqkt0YGLluy4oiB4plUdRLMy+lcXTm";""") \
#     .load()

stream_df = df.selectExpr("CAST(value AS STRING)")

stream_df = stream_df.withColumn("value", from_json("value", schema=schema)).select(col("value.*"))

stream_df.writeStream \
    .format("console") \
    .outputMode("update") \
    .start() \
    .awaitTermination()
