import os
from pyspark.sql import SparkSession


def test_spark_session():
    spark = (SparkSession.builder
        .master("local[*]")
        .appName("test")
        .getOrCreate())
    assert spark.version.startswith("3")

