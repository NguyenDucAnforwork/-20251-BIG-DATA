from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, sum as f_sum, avg, stddev_samp


spark = SparkSession.builder.appName("ts-aggregations").getOrCreate()


curated = spark.read.parquet("hdfs://namenode:8020/curated/payments/")


daily = (curated
.groupBy("merchant_id", to_date("event_date").alias("dt"))
.agg(f_sum("sum_amt").alias("sum_amt")))


# 7‑day moving average & z‑score anomaly
w = (Window.partitionBy("merchant_id").orderBy("dt").rowsBetween(-6, 0))
with_ma = (daily
    .withColumn("ma7", avg("sum_amt").over(w))
    .withColumn("sd7", stddev_samp("sum_amt").over(w))
    .withColumn("z", (col("sum_amt") - col("ma7")) / (col("sd7") + 1e-9)))


with_ma.write.mode("overwrite").parquet("hdfs://namenode:8020/timeseries/daily_ma/")