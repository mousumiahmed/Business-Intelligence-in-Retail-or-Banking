from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, LongType

spark = SparkSession.builder.appName("streaming_txn").getOrCreate()

schema = StructType() \
    .add("txn_id", StringType()) \
    .add("customer_id", StringType()) \
    .add("amount", DoubleType()) \
    .add("timestamp", LongType()) \
    .add("payment_type", StringType()) \
    .add("store_id", StringType()) \
    .add("device", StringType())

kafka_bootstrap = "localhost:9092"  # replace with broker endpoints
df_raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "earliest") \
    .load()

df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")).select("data.*") \
    .withColumn("event_time", to_timestamp((col("timestamp")/1000).cast("long")))

# write raw stream to Delta (partition by date)
output_path_raw = "/mnt/delta/raw_transactions"
df_parsed.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/mnt/delta/checkpoints/raw_txn") \
    .outputMode("append") \
    .start(output_path_raw)
