from pyspark.sql import DataFrame
from pyspark.sql.functions import col


# Simple utility to enforce positive amounts and currency upper-case
from pyspark.sql.functions import upper, abs as f_abs


def cleanse_payments(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("amount", f_abs(col("amount")))
        .withColumn("currency", upper(col("currency")))
    )