# Databricks notebook source
dbutils.widgets.text("p_data_source","")
v_data_soruce = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType,StringType

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, lit

# COMMAND ----------

pip_stops_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("stop", StringType(), True),
    StructField("lap", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
])

# COMMAND ----------

pit_stops_df = spark.read\
.schema(pip_stops_schema)\
.option("multiline",True)\
.json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

columns_add_df = pit_stops_df.withColumnRenamed("driverId","driver_id")\
                       .withColumnRenamed("raceId","race_id")\
                       .withColumn("data_source", lit(v_data_soruce))

# COMMAND ----------

final_df = add_ingestion_date(columns_add_df)

# COMMAND ----------

# final_df.write.mode("overwrite").format("parquet").saveAsTable('f1_processed.pit_stops')
overwrite_partition(final_df,"f1_processed","pit_stops","race_id")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/pit_stops"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.pit_stops;

# COMMAND ----------

dbutils.notebook.exit("Success")
