-- Databricks notebook source
-- MAGIC %md
-- MAGIC In this repo we will cover some topics related to Spark SQL and Some basic transformations.

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS PRIMARY_DB;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC fire_df = spark.read\
-- MAGIC     .format("csv")\
-- MAGIC     .option("header","true")\
-- MAGIC     .option("inferSchema","true")\
-- MAGIC     .load("dbfs:/FileStore/tables/WA_Fn_UseC__Telco_Customer_Churn.csv")
-- MAGIC fire_df.printSchema()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC fire_df.write.format("parquet").saveAsTable("PRIMARY_DB.FIRE_TABLE")

-- COMMAND ----------

SELECT * from primary_db.fire_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Spark Dataframe
-- MAGIC
-- MAGIC Spark dataframe is stored three layers:
-- MAGIC 1. Spark Metadata that is stored in Databricks
-- MAGIC 2. File Format type stored in Cloud storage in a datafike
-- MAGIC 4. Compute layers run the spark SQL engine and Spark Dataframe APIs.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ```Q4. How much distince types of Payements were made to the fire department?```
-- MAGIC
-- MAGIC We will use SQL distinct method that will provide us with the unique values.
-- MAGIC

-- COMMAND ----------

Select count (distinct PaymentMethod) from primary_db.fire_table where PaymentMethod is not null;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ```Q5. What where the distinct types of call made to the fire department?```

-- COMMAND ----------

Select  distinct  PaymentMethod FROM primary_db.fire_table where PaymentMethod is not null;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ```Q5. What will be the avg MonthlyCharges for each distinct PayementMethod?```

-- COMMAND ----------

SELECT  avg(MonthlyCharges), PaymentMethod from primary_db.fire_table GROUP BY PaymentMethod

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ```Q6. Same as Q5 what will be max and min of both Monthly charges and Total Charges```

-- COMMAND ----------

Select PaymentMethod,
max(MonthlyCharges) as MaxMonthlyCharges ,
min(MonthlyCharges) as MinMonthlyCharges,
avg(MonthlyCharges) as AvgMonthlyCharges,
max(TotalCharges) as MaxTotalCharges ,
min(TotalCharges) as MinTotalCharges,
avg(TotalCharges) as AvgTotalCharges
from primary_db.fire_table
  group by PaymentMethod;

-- COMMAND ----------


