import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
col, from_json, to_timestamp, window, expr, when, approx_count_distinct,
substring, lit
)
from pyspark.sql.types import TimestampType
from pyspark.sql.streaming import GroupState, GroupStateTimeout
from common.schemas import payment_event_schema
from common.utils import cleanse_payments


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "payments.events")
CHECKPOINT = os.getenv("CHECKPOINT", "hdfs://namenode:8020/checkpoints/streaming_tx")
OUT_BRONZE = os.getenv("OUT_BRONZE", "hdfs://namenode:8020/bronze/payments/")
OUT_CASSANDRA_KEYSPACE = os.getenv("CS_KEYSPACE", "payments")
OUT_CASSANDRA_TABLE = os.getenv("CS_TABLE", "agg_5m_by_merchant")


spark = (SparkSession.builder
    .appName("streaming-transactions")
    .config("spark.sql.shuffle.partitions", os.getenv("SHUFFLE", "200"))
    .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
    .getOrCreate())


spark.sparkContext.setLogLevel("WARN")


# 1) Read from Kafka (Structured Streaming)
df_raw = (spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load())


parsed = (df_raw
    .selectExpr("CAST(value AS STRING) AS json", "timestamp")
    .select(from_json(col("json"), payment_event_schema).alias("e"), col("timestamp").alias("ingest_ts"))
    .select("e.*", "ingest_ts"))


# 2) Cleanse + event time
enriched = (cleanse_payments(parsed)
    .withColumn("event_time", to_timestamp((col("ts_ms")/1000).cast("timestamp")))
    .withWatermark("event_time", "20 minutes")) # watermark for late data


# 3) Dedup exactly-once at event level (idempotent write downstream by primary key)
# Use last status by event_id if duplicates arrive
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

w = Window.partitionBy("event_id").orderBy(col("event_time").desc())
unique_events = (enriched
    .withColumn("rn", row_number().over(w))
    .filter(col("rn") == 1)
    .drop("rn"))

# 4) Write raw bronze to HDFS with checkpoint (append is exactly-once per batch given checkpoint)
bronze_query = (unique_events
    .writeStream
    .format("parquet")
    .option("path", OUT_BRONZE)
    .option("checkpointLocation", f"{CHECKPOINT}/bronze")
    .outputMode("append")
    .start())

# 5) Aggregations: 5‑minute sliding windows per merchant/status
agg5m = (unique_events
    .groupBy(
    window(col("event_time"), "5 minutes", "1 minute"),
    col("merchant_id"),
    col("status")
    )
    .agg(
    expr("count(*) as cnt"),
    expr("sum(amount) as sum_amount"),
    approx_count_distinct("user_id").alias("n_users")
    )
    .withColumn("window_start", col("window.start"))
    .withColumn("window_end", col("window.end"))
    .drop("window"))

# 6) Stateful example: track rolling failure rate per merchant
from pyspark.sql.functions import struct


state_schema = "merchant_id string, total long, failed long, failure_rate double"


def update_state(merchant_id, rows, state: GroupState):
    total = state.get("total") if state.exists else 0
    failed = state.get("failed") if state.exists else 0
    for r in rows:
    total += 1
    if r.status == "failed":
    failed += 1
    fr = (failed / total) if total else 0.0
    state.update({"total": total, "failed": failed})
    return (merchant_id, total, failed, fr)


from pyspark.sql.functions import expr as sql_expr


stream_for_state = unique_events.select("merchant_id", "status", "event_time").withWatermark("event_time", "20 minutes")


stateful = (stream_for_state
    .groupByKey(lambda r: r.merchant_id)
    .mapGroupsWithState(update_state, outputMode="update", timeoutConf=GroupStateTimeout.ProcessingTimeTimeout()))

# 7) Sink aggregates to Cassandra (idempotent on composite primary key)


def write_cassandra(batch_df, batch_id: int):
    (batch_df
    .write
    .format("org.apache.spark.sql.cassandra")
    .mode("append")
    .options(table=OUT_CASSANDRA_TABLE, keyspace=OUT_CASSANDRA_KEYSPACE)
    .save())


cass_query = (agg5m
    .writeStream
    .foreachBatch(write_cassandra) # exactly-once with idempotent primary key
    .outputMode("update")
    .option("checkpointLocation", f"{CHECKPOINT}/agg5m")
    .start())


spark.streams.awaitAnyTermination()