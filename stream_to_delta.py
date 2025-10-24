import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, to_date
from pyspark.sql.types import StructType, StringType, DoubleType, LongType


def main(kafka_bootstrap, kafka_topic, output_path, checkpoint_path):
    spark = SparkSession.builder \
    .appName("streaming_txn") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()


    schema = StructType() \
        .add("txn_id", StringType()) \
        .add("customer_id", StringType()) \
        .add("amount", DoubleType()) \
        .add("timestamp", LongType()) \
        .add("payment_type", StringType()) \
        .add("store_id", StringType()) \
        .add("device", StringType())

    df_raw = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .load()

    df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), schema).alias("data")).select("data.*") \
        .withColumn("event_time", to_timestamp((col("timestamp")/1000).cast("long"))) \
        .withColumn("date", to_date(col("event_time")))

    query = df_parsed.writeStream \
        .format("delta") \
        .option("checkpointLocation", checkpoint_path) \
        .outputMode("append") \
        .start(output_path)

    query.awaitTermination()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--kafka-bootstrap', required=True)
    parser.add_argument('--kafka-topic', default='transactions')
    parser.add_argument('--output-path', required=True)
    parser.add_argument('--checkpoint-path', required=True)
    args = parser.parse_args()
    main(args.kafka_bootstrap, args.kafka_topic, args.output_path, args.checkpoint_path)