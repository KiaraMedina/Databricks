# Databricks notebook source
dbutils.widgets.text("p_data_source","")
v_data_soruce = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType,IntegerType, StringType, DateType, StructField

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,to_timestamp, col, concat, lit

# COMMAND ----------

racesSchema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

races_df = spark.read\
.option("header",True)\
.schema(racesSchema)\
.csv(f"{raw_folder_path}/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Add ingestion date and race_timestamp to the dataframe

# COMMAND ----------

races_with_timestamp_df = add_ingestion_date(races_df)

# COMMAND ----------

races_selected_df = races_with_timestamp_df\
.withColumn("race_timestamp", to_timestamp(concat(col('date'),lit(' '), col('time')), 'yyy-MM-dd HH:mm:ss' ))\
.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("year","race_year")\
.withColumnRenamed("circuitId","circuit_id")\
.withColumn("data_source", lit(v_data_soruce))

# COMMAND ----------

races_with_timestamp_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write the output to processed container in parquet format

# COMMAND ----------

races_selected_df.write.mode("overwrite").partitionBy("race_year").parquet(f"{processed_folder_path}/race")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/race"))

# COMMAND ----------

dbutils.notebook.exit("Success")
