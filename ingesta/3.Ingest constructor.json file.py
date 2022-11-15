# Databricks notebook source
constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df= spark.read\
.schema(constructors_schema)\
.json("/mnt/storageformula1dl/raw/constructors.json")

# COMMAND ----------

constructor_df.printSchema()

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Drop unwanted columns from the dataframe

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

constructor_dropped_df = constructor_df.drop(col("url"))

# COMMAND ----------

display(constructor_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Rename columns and add ingestion date

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId","constructor_id")\
                                             .withColumnRenamed("constructorRef","contructor_ref")\
                                             .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write output to parquet file

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet("/mnt/storageformula1dl/processed/constructor")

# COMMAND ----------

display(spark.read.parquet("/mnt/storageformula1dl/processed/constructor"))

# COMMAND ----------


