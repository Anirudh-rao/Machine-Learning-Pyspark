# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #Linear Regression with Python

# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd
import pylab as pl
import numpy as np
%matplotlib inline
from sklearn import linear_model
from pyspark.sql import SparkSession

#assigning linear_model to a variable
rgr = linear_model.LinearRegression()

spark = SparkSession.builder.appName('lr_example').getOrCreate()


# COMMAND ----------

df = spark.read.table("works.default.ecommerce_customers")
df.show()
df = df.toPandas()

# COMMAND ----------

# Selecting some randoms columns for regression
cdf = df[["Avg Session Length","Time on App","Time on Website","Length of Membership","Yearly Amount Spent"]]
cdf.head(3)

# COMMAND ----------

plt.scatter(cdf[["Length of Membership"]], cdf[["Yearly Amount Spent"]],  color='blue')
plt.xlabel("Length of Membership")
plt.ylabel("Yearly Amount Spent")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC In reality, there are **multiple variables** that predict the Yearly Amount Spent. When more than one independent variable is present, the process is called multiple linear regression. For example, predicting Yearly Amount Spent using Avg Session Length, Time on App, Time on Website and Length of Membership. The good thing here is that Multiple linear regression is the extension of simple linear regression model.
# MAGIC
# MAGIC ## Creating train and test dataset
# MAGIC Train/Test Split involves splitting the dataset into training and testing sets respectively, which are mutually exclusive. After which, you train with the training set and test with the testing set.

# COMMAND ----------

msk = np.random.rand(len(df)) < 0.8

# COMMAND ----------

train  = cdf[msk]
test = cdf[~msk]

# COMMAND ----------


#### Train data distribution
plt.scatter(train[["Length of Membership"]], train[["Yearly Amount Spent"]],  color='blue')
plt.xlabel("Length of Membership")
plt.ylabel("Yearly Amount Spent")
plt.show()

# COMMAND ----------

input_cols = ["Avg Session Length","Time on App","Time on Website","Length of Membership"]

# COMMAND ----------

X = np.asanyarray(train[input_cols])
y = np.asanyarray(train[["Yearly Amount Spent"]])

# COMMAND ----------

rgr.fit(X,y)
print('Coefficients :', rgr.coef_)

# COMMAND ----------

# MAGIC %md
# MAGIC **Coefficient** and **Intercept** , are the parameters of the fit line. Given that it is a multiple linear regression, with 3 parameters, and knowing that the parameters are the intercept and coefficients of hyperplane, sklearn can estimate them from our data. Scikit-learn uses plain Ordinary Least Squares method to solve this problem.
# MAGIC
# MAGIC ## Prediction

# COMMAND ----------

y_hat = rgr.predict(test[input_cols])
x = np.asanyarray(test[input_cols])
y = np.asanyarray(test[['Yearly Amount Spent']])

print('Residual sum of squares: %.2f'% np.mean((y_hat-y)**2))

# Explained variance score: 1 is perfect prediction
print('Variance score: %.2f' % rgr.score(x, y))

# COMMAND ----------

# MAGIC %md
# MAGIC **explained variance regression score:**
# MAGIC If `Y_hat` is the estimated target output, `y`the corresponding (correct) target output, and `Var` is Variance, the square of the standard deviation.The best possible score is 1.0 , lower values are worse.

# COMMAND ----------

# MAGIC %md
# MAGIC # Linear Regression with Pyspark

# COMMAND ----------

data = spark.read.table('works.default.ecommerce_customers')
display(data)

# COMMAND ----------

data.printSchema()

# COMMAND ----------

from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler

# COMMAND ----------

data.columns

# COMMAND ----------

assembler = VectorAssembler(
    inputCols=["Avg Session Length", "Time on App", 
               "Time on Website","Length of Membership"],
    outputCol="features")


# COMMAND ----------

output = assembler.transform(data)
output.select("features").show(3)


# COMMAND ----------

final_data = output.select("features",'Yearly Amount Spent')

# COMMAND ----------


# Pass in the split between training/test as a list.
train_data,test_data = final_data.randomSplit([0.7,0.3])

# COMMAND ----------

train_data,test_data = final_data.randomSplit([0.7,0.3])


# COMMAND ----------

lr = LinearRegression(labelCol='Yearly Amount Spent')


# COMMAND ----------

lrModel = lr.fit(train_data,)


# COMMAND ----------

# Print the coefficients and intercept for linear regression
print("Coefficients: {} Intercept: {}".format(lrModel.coefficients,lrModel.intercept))

# COMMAND ----------

test_results = lrModel.evaluate(test_data)


# COMMAND ----------

print("RMSE: {}".format(test_results.rootMeanSquaredError))


# COMMAND ----------

unlabeled_data = test_data.select('features')


# COMMAND ----------

predictions = lrModel.transform(unlabeled_data)


# COMMAND ----------

print("RMSE: {}".format(test_results.rootMeanSquaredError))
print("MSE: {}".format(test_results.meanSquaredError))