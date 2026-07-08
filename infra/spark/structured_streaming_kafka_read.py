# Phase 0 pilot: minimal Structured Streaming job proving a real consumer reads from the
# Kafka broker built earlier in Phase 0. Not the final feature-engineering pipeline --
# that comes with the fault-injection campaign (RO2). This just proves the wiring works
# and gives Spark real, moving data so its own metrics (processedRowsPerSecond etc.) are
# non-trivial when scraped by Prometheus.
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count, to_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("kspfail-streaming-pilot").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("seq", IntegerType()),
    StructField("ts", StringType()),
    StructField("value", IntegerType()),
])

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kspfail-kafka-bootstrap.kafka.svc.cluster.local:9092")
    .option("subscribe", "pipeline-events")
    .option("startingOffsets", "earliest")
    .load()
)

parsed = (
    raw.select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .withColumn("ts", to_timestamp(col("ts")))
)

windowed_counts = (
    parsed
    .withWatermark("ts", "0 seconds")
    .groupBy(window(col("ts"), "10 seconds"))
    .agg(count("*").alias("event_count"))
)

query = (
    windowed_counts.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", "/tmp/kspfail-checkpoint")
    .trigger(processingTime="5 seconds")
    .start()
)

query.awaitTermination()
