# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest drivers.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Read the json file using the spark dataframe reader API

# COMMAND ----------

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

from pyspark.sql.types import StructType, StructField,IntegerType, StringType,DateType


# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit, concat

# COMMAND ----------

name_schema= StructType(fields =[
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True)
])

# COMMAND ----------

drives_schema = StructType(fields=[
    StructField("driverId", IntegerType(),True),
    StructField("driverRef", StringType(),True),
    StructField("number", IntegerType(),True),
    StructField("code", StringType(),True),
    StructField("name", name_schema),
    StructField("dob", DateType(),True),
    StructField("nationality", StringType(),True),
    StructField("url", StringType(),True)
])

# COMMAND ----------

drivers_df = spark.read\
.schema(drives_schema)\
.json(f"{raw_folder_path}/{v_file_date}/drivers.json")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Rename columns and add new columns

# COMMAND ----------

drivers_with_columns_df = add_ingestion_date(drivers_df)

# COMMAND ----------

drivers_with_columns_df = drivers_df.withColumnRenamed("driverId","driver_id")\
                                .withColumnRenamed("driverRef","driver_ref")\
                                .withColumn("name", concat(col("name.forename"),lit(" "), col("name.surname")))\
                                .withColumn("data_source", lit(v_data_soruce))\
                                .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop(col("url"))

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("delta").saveAsTable('f1_processed.drivers')

# COMMAND ----------

# display(spark.read.parquet(f"{processed_folder_path}/drivers"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.drivers;

# COMMAND ----------

dbutils.notebook.exit("Success")
