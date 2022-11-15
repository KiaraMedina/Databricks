# Databricks notebook source
from pyspark.sql.types import StructType,IntegerType, StringType, DateType, StructField

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
.csv("/mnt/storageformula1dl/raw/races.csv")

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Add ingestion date and race_timestamp to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,to_timestamp, col, concat, lit

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("ingestion", current_timestamp())\
                                  .withColumn("race_timestamp", to_timestamp(concat(col('date'),lit(' '), col('time')), 'yyy-MM-dd HH:mm:ss' ))

# COMMAND ----------

display(races_with_timestamp_df)

# COMMAND ----------

races_selected_df=races_with_timestamp_df.select(col("raceId").alias("race_id"),col("year").alias("race_year"), col("round"), col("circuitId").alias("circuit_id"),col("name"),col("ingestion").alias("ingestion_date"), col("race_timestamp"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write the output to processed container in parquet format

# COMMAND ----------

races_selected_df.write.mode("overwrite").partitionBy("race_year").parquet("/mnt/storageformula1dl/processed/race")

# COMMAND ----------

df= spark.read.parquet("/mnt/storageformula1dl/processed/race")

# COMMAND ----------

display(df)
