# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest drivers.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Read the json file using the spark dataframe reader API

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
.json("/mnt/storageformula1dl/raw/drivers.json")


# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Rename columns and add new columns

# COMMAND ----------

drivers_df.show()

# COMMAND ----------

drivers_with_columns_df = drivers_df.withColumnRenamed("driverId","driver_id")\
                                .withColumnRenamed("driverRef","driver_ref")\
                                .withColumn("ingestion_date",current_timestamp())\
                                .withColumn("name", concat(col("name.forename"),lit(" "), col("name.surname")))

# COMMAND ----------

display(drivers_with_columns_df)

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop(col("url"))

# COMMAND ----------

drivers_final_df.write.mode("overwrite").parquet("/mnt/storageformula1dl/processed/drivers")

# COMMAND ----------

display(spark.read.parquet("/mnt/storageformula1dl/processed/drivers"))
