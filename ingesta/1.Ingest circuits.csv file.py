# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType,StringType,BooleanType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields= [
    StructField('circuitId', IntegerType(), True),
    StructField('circuitRef', StringType(),True),
    StructField('name', StringType(),True),
    StructField('location', StringType(),True),
    StructField('country', StringType(),True),
    StructField('lat', DoubleType(),True ),
    StructField('lng', DoubleType(),True),
    StructField('alt', IntegerType(),True),
    StructField('url', StringType(),True),
])

# COMMAND ----------

circuits_df = spark.read\
.option("header",True)\
.schema(circuits_schema)\
.csv("/mnt/storageformula1dl/raw/circuits.csv")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.columns

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Select only the required columns

# COMMAND ----------

from pyspark.sql.functions import col , current_timestamp

# COMMAND ----------

circuits_select_df = circuits_df.select("circuitId","circuitRef","name","location","country","lat","lng","alt")
# circuits_select_df = circuits_df.select(circuits_df["circuitId"].alias("id"),circuits_df["circuitRef"]).show()
# circuits_select_df = circuits_df.select(col("circuitId").alias("id"),col("circuitRef")).show()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename the column 

# COMMAND ----------

circuits_renamed_df = circuits_select_df.withColumnRenamed("circuitId","id")

# COMMAND ----------

circuits_df_agg = circuits_renamed_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(circuits_df_agg)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write data to datalake as parquet

# COMMAND ----------

circuits_df_agg.write.parquet("mnt/storageformula1dl/processed/circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/storageformula1dl/processed/circuits

# COMMAND ----------

df= spark.read.parquet("/mnt/storageformula1dl/processed/circuits")

# COMMAND ----------

display(df)
