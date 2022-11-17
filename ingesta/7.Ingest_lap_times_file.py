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
.csv(f"{raw_folder_path}/{v_file_date}/lap_times")

# COMMAND ----------

columns_add_df = lap_times_df.withColumnRenamed("driverId","driver_id")\
                       .withColumnRenamed("raceId","race_id")\
                       .withColumn("data_source", lit(v_data_soruce))\
                       .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

final_df= add_ingestion_date(columns_add_df)

# COMMAND ----------

# final_df.write.mode("overwrite").format("parquet").saveAsTable('f1_processed.lap_times')
overwrite_partition(final_df,"f1_processed","lap_times","race_id")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/lap_times"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.lap_times;

# COMMAND ----------

dbutils.notebook.exit("Success")
