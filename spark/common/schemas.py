from pyspark.sql.types import (
StructType, StructField, StringType, LongType, DoubleType, MapType
)


payment_event_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("ts_ms", LongType(), False),
    StructField("user_id", StringType(), False),
    StructField("merchant_id", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("currency", StringType(), True),
    StructField("country", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("status", StringType(), False),
    StructField("metadata", MapType(StringType(), StringType()), True),
    StructField("ingest_received_ms", LongType(), True),
])