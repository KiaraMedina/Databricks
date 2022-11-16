# Databricks notebook source
dbutils.widgets.text("p_data_source","")
v_data_soruce = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType,StringType,BooleanType, DoubleType

# COMMAND ----------

from pyspark.sql.functions import col , current_timestamp, lit

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
.csv(f"{raw_folder_path}/circuits.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Select only the required columns

# COMMAND ----------

circuits_select_df = circuits_df.select("circuitId","circuitRef","name","location","country","lat","lng","alt")
# circuits_select_df = circuits_df.select(circuits_df["circuitId"].alias("id"),circuits_df["circuitRef"]).show()
# circuits_select_df = circuits_df.select(col("circuitId").alias("id"),col("circuitRef")).show()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename the column 

# COMMAND ----------

circuits_renamed_df = circuits_select_df.withColumnRenamed("circuitId","circuit_id")\
.withColumnRenamed("circuitRef","circuit_ref")\
.withColumnRenamed("lat",'latitude')\
.withColumnRenamed("lng","longitude")\
.withColumnRenamed("alt","altitude")\
.withColumn("data_source", lit(v_data_soruce))

# COMMAND ----------

circuits_final_df= add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write data to datalake as parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/circuits"))

# COMMAND ----------

dbutils.notebook.exit("Success")
