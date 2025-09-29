from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("ge-validate").getOrCreate()
df = spark.read.parquet("hdfs://namenode:8020/bronze/payments/")


gx = SparkDFDataset(df)
res = gx.expect_column_values_to_not_be_null("event_id")
res &= gx.expect_column_values_to_be_between("amount", min_value=0)
print("Validation results:", res)