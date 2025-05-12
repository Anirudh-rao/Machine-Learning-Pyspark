# Databricks notebook source
import pyspark 
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName("Aggregations").getOrCreate()


# COMMAND ----------

df = spark.createDataFrame(
 [(1,"north",100,"walmart"),(2,"south",300,"apple"),
 (3,"west",200,"google"),(1,"east",200,"google"),
 (2,"north",100,"walmart"),(3,"west",300,"apple"),
 (1,"north",200,"walmart"),(2,"east",500,"google"), 
(3,"west",400,"apple"),],["emp_id","region","sales","customer"])

# COMMAND ----------

df.show()

# COMMAND ----------

df.agg({"sales":"sum"}).show()

# COMMAND ----------

df.agg({"sales": "min"}).show()

# COMMAND ----------

df.agg({"sales": "max"}).show()


# COMMAND ----------

df.agg({"sales": "count"}).show()


# COMMAND ----------

df.agg({"sales": "mean"}).show()


# COMMAND ----------

df.agg({"sales": "mean","customer":"count"}).show()
