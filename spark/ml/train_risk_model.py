import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder


CURATED = os.getenv("CURATED", "hdfs://namenode:8020/curated/payments/")
MODEL_DIR = os.getenv("MODEL_DIR", "hdfs://namenode:8020/models/risk_lr/")


spark = SparkSession.builder.appName("train-risk-model").getOrCreate()


# Load curated data (long format)
df = spark.read.parquet(CURATED)


# Label: bất thường khi sum_amt lớn nhưng status=failed (minh hoạ)
df = df.withColumn("label", when((col("status") == "failed") & (col("sum_amt") > 50), 1.0).otherwise(0.0))


cat_cols = ["status"]
num_cols = ["sum_amt"]


indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") for c in cat_cols]
encoders = [OneHotEncoder(inputCols=[f"{c}_idx"], outputCols=[f"{c}_oh"]) for c in cat_cols]


assembler = VectorAssembler(inputCols=["sum_amt", "status_oh"], outputCol="features_raw")
scaler = StandardScaler(inputCol="features_raw", outputCol="features")


lr = LogisticRegression(featuresCol="features", labelCol="label")


pipeline = Pipeline(stages=[*indexers, *encoders, assembler, scaler, lr])


paramGrid = (ParamGridBuilder()
.addGrid(lr.regParam, [0.0, 0.01, 0.1])
.addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0])
.build())


evalr = BinaryClassificationEvaluator(labelCol="label")


cv = CrossValidator(estimator=pipeline, estimatorParamMaps=paramGrid, evaluator=evalr, numFolds=3)


model = cv.fit(df)


model.bestModel.write().overwrite().save(MODEL_DIR)
print("Saved model to:", MODEL_DIR)