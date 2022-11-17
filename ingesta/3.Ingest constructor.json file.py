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

from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df= spark.read\
.schema(constructors_schema)\
.json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Drop unwanted columns from the dataframe

# COMMAND ----------

constructor_dropped_df = constructor_df.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Rename columns and add ingestion date

# COMMAND ----------

constructor_final_df = add_ingestion_date(constructor_dropped_df)

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId","constructor_id")\
                                             .withColumnRenamed("constructorRef","contructor_ref")\
                                             .withColumn("data_source", lit(v_data_soruce))\
                                             .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write output to parquet file

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("delta").saveAsTable('f1_processed.constructor')

# COMMAND ----------

# display(spark.read.parquet(f"{processed_folder_path}/constructor"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.constructor;

# COMMAND ----------

dbutils.notebook.exit("Success")
