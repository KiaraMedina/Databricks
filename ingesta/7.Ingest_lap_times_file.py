# Databricks notebook source
dbutils.widgets.text("p_data_source","")
v_data_soruce = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType,StringType

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, lit

# COMMAND ----------

lap_times_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
])

# COMMAND ----------

lap_times_df = spark.read\
.schema(lap_times_schema)\
.csv(f"{raw_folder_path}/lap_times")

# COMMAND ----------

columns_add_df = lap_times_df.withColumnRenamed("driverId","driver_id")\
                       .withColumnRenamed("raceId","race_id")\
                       .withColumn("data_source", lit(v_data_soruce))

# COMMAND ----------

final_df= add_ingestion_date(columns_add_df)

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/lap_times")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/lap_times"))

# COMMAND ----------

dbutils.notebook.exit("Success")
