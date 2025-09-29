import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as f_sum, countDistinct, expr, when, avg, regexp_replace
from pyspark.sql.window import Window


BRONZE = os.getenv("BRONZE", "hdfs://namenode:8020/bronze/payments/")
CURATED = os.getenv("CURATED", "hdfs://namenode:8020/curated/payments/")
DIM_PATH = os.getenv("DIM", "hdfs://namenode:8020/dim/")

spark = (SparkSession.builder
    .appName("batch-transformations")
    .config("spark.sql.shuffle.partitions", os.getenv("SHUFFLE", "200"))
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")


raw = spark.read.parquet(BRONZE)


# Custom UDF: risk tag (demonstration) — prefer pandas UDF for perf
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

@udf(StringType())
def risk_bucket(amount: float, status: str) -> str:
    if status == "failed":
        return "risk_high" if amount >= 100 else "risk_med"
    return "risk_low"


df = raw.withColumn("risk_bucket", risk_bucket(col("amount"), col("status")))

# Join with dimensions (broadcast for small dim)
merchant_dim = spark.read.parquet(f"{DIM_PATH}/merchants/")
user_dim = spark.read.parquet(f"{DIM_PATH}/users/")


df = (df
    .join(merchant_dim.hint("broadcast"), "merchant_id", "left")
    .join(user_dim, "user_id", "left"))


# Window functions: rolling daily amount per user
from pyspark.sql.functions import to_date
w = Window.partitionBy("user_id").orderBy("event_time").rowsBetween(-10, 0)


df = df.withColumn("rolling_amt_10", expr("sum(amount) over w")).withColumn("event_date", to_date("event_time"))

# Advanced aggregation with pivot
agg = (df.groupBy("event_date", "merchant_id")
    .pivot("status", ["authorized", "captured", "failed"]) # pivot on status
    .agg(f_sum("amount").alias("sum_amt")))


# Unpivot (stack) back to long format
unpivot_cols = ["authorized_sum_amt", "captured_sum_amt", "failed_sum_amt"]
unpivot_expr = "stack(3, 'authorized', authorized_sum_amt, 'captured', captured_sum_amt, 'failed', failed_sum_amt) as (status, sum_amt)"


unpivoted = (agg.select("event_date", "merchant_id", *unpivot_cols)
.selectExpr("event_date", "merchant_id", unpivot_expr))

# Bucketing for future sort-merge joins
(unpivoted
    .repartition(200)
    .write
    .format("parquet")
    .mode("overwrite")
    .bucketBy(64, "merchant_id")
    .sortBy("merchant_id")
    .saveAsTable("curated_payments_bucketed"))


# Cache strategic & explain
unpivoted.cache()
print(unpivoted.explain(True))

# Output curated
(unpivoted
    .write.mode("overwrite")
    .partitionBy("event_date")
    .parquet(CURATED))