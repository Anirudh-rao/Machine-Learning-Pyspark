# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
# Create a Spark session
spark = SparkSession.builder.appName("Data Joining").getOrCreate()

# COMMAND ----------

df_left = spark.createDataFrame([(1001,1,100),(1002,2,200), 
(1003,3,300),(1004,1,200),(1005,6,200)],["order_id","customer_id","amount"])
df_left.show()

# COMMAND ----------

df_right = spark.createDataFrame([(1,"john"), 
(2,"mike"),(3,"tony"),(4,"kent")],["customer_id","name"])
df_right.show()

# COMMAND ----------

df_right.join(df_left, on='customer_id',how="inner").show()

# COMMAND ----------

df_left.join(df_right,on="customer_id",how="left").show()
df_left.join(df_right,on="customer_id",how="left_outer").show()
df_left.join(df_right,on="customer_id",how="leftouter").show()

# COMMAND ----------

df_left.join(df_right,on="customer_id",how="full").show()
df_left.join(df_right,on="customer_id",how="fullouter").show()
df_left.join(df_right,on="customer_id",how="full_outer").show()

# COMMAND ----------

spark.conf.set("spark.sql.crossJoin.enabled", "true")
df_left.crossJoin(df_right).show()

# COMMAND ----------

df_left.join(df_right,on="customer_id",how="semi").show()


# COMMAND ----------

df_left.join(df_right,on="customer_id",how="leftsemi").show()


# COMMAND ----------

df_left.join(df_right,on="customer_id",how="left_semi").show()


# COMMAND ----------

df_left.join(df_right,on="customer_id",how="anti").show()
df_left.join(df_right,on="customer_id",how="leftanti").show()
df_left.join(df_right,on="customer_id",how="left_anti").show()