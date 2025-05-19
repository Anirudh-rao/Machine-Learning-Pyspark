# Databricks notebook source
import pandas as pd
from datetime import datetime, date
from pyspark.sql import Row

data_df = spark.createDataFrame([
    Row(col_1=100, col_2=200., col_3='string_test_1', col_4=date(2023, 1, 1), col_5=datetime(2023, 1, 1, 12, 0)),
    Row(col_1=200, col_2=300., col_3='string_test_2', col_4=date(2023, 2, 1), col_5=datetime(2023, 1, 2, 12, 0)),
    Row(col_1=400, col_2=500., col_3='string_test_3', col_4=date(2023, 3, 1), col_5=datetime(2023, 1, 3, 12, 0))
])


# COMMAND ----------

import pandas as pd
from datetime import datetime, date
from pyspark.sql import Row

data_df = spark.createDataFrame([
    Row(col_1=100, col_2=200., col_3='string_test_1', col_4=date(2023, 1, 1), col_5=datetime(2023, 1, 1, 12, 0)),
    Row(col_1=200, col_2=300., col_3='string_test_2', col_4=date(2023, 2, 1), col_5=datetime(2023, 1, 2, 12, 0)),
    Row(col_1=400, col_2=500., col_3='string_test_3', col_4=date(2023, 3, 1), col_5=datetime(2023, 1, 3, 12, 0))
], schema=' col_1 long, col_2 double, col_3 string, col_4 date, col_5 timestamp')

# COMMAND ----------


import pandas as pd
from datetime import datetime, date
from pyspark.sql import Row

pandas_df = pd.DataFrame({
    'col_1': [100, 200, 400],
    'col_2': [200., 300., 500.],
    'col_3': ['string_test_1', 'string_test_2', 'string_test_3'],
    'col_4': [date(2023, 1, 1), date(2023, 2, 1), date(2023, 3, 1)],
    'col_5': [datetime(2023, 1, 1, 12, 0), datetime(2023, 1, 2, 12, 0), datetime(2023, 1, 3, 12, 0)]
})
df = spark.createDataFrame(pandas_df)

# COMMAND ----------

df.show()

# COMMAND ----------

df.show(2)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.show(1, vertical=True)

# COMMAND ----------

df.columns

# COMMAND ----------

df.select('col_1','col_2','col_3').describe().show()

# COMMAND ----------

df.collect()