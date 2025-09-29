from pyspark.sql import SparkSession
from pyspark.sql.functions import col
# from graphframes import GraphFrame # ensure proper package installed


spark = (SparkSession.builder
.appName("fraud-graph")
.getOrCreate())


# Load edges from curated transactions (user -> merchant, user -> device)
curated = spark.read.parquet("hdfs://namenode:8020/curated/payments/")


users = curated.select(col("user_id").alias("id")).distinct()
merchants = curated.select(col("merchant_id").alias("id")).distinct()
devices = curated.select(col("device_id").alias("id")).distinct()


vertices = users.unionByName(merchants).unionByName(devices).distinct()


u2m = curated.select(col("user_id").alias("src"), col("merchant_id").alias("dst")).distinct()
u2d = curated.select(col("user_id").alias("src"), col("device_id").alias("dst")).distinct()
edges = u2m.unionByName(u2d)


# g = GraphFrame(vertices, edges)
# cc = g.connectedComponents()
# tri = g.triangleCount()
# pr = g.pageRank(resetProbability=0.15, maxIter=10)


# Example output sinks (disabled if graphframes missing)
# cc.write.mode("overwrite").parquet("hdfs://namenode:8020/graph/cc/")
# tri.write.mode("overwrite").parquet("hdfs://namenode:8020/graph/tri/")
# pr.vertices.write.mode("overwrite").parquet("hdfs://namenode:8020/graph/pr/")


print("Graph analytics job template ready.")