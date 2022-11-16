# Databricks notebook source
dbutils.widgets.text("p_data_source","")
v_data_soruce = dbutils.widgets.get("p_data_source")


# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType,FloatType,StringType

# COMMAND ----------

from pyspark.sql.functions import  current_timestamp, lit, col

# COMMAND ----------

results_schema = StructType(fields=[
    StructField("resultId",IntegerType(), False),
    StructField("raceId",IntegerType(), False),
    StructField("driverId",IntegerType(), False),
    StructField("constructorId",IntegerType(), False),
    StructField("number",IntegerType(), False),
    StructField("grid",IntegerType(), False),
    StructField("position",IntegerType(), False),
    StructField("positionText",IntegerType(), False),
    StructField("positionOrder",IntegerType(), False),
    StructField("points",FloatType(), False),
    StructField("laps",IntegerType(), False),
    StructField("time",StringType(), False),
    StructField("milliseconds",IntegerType(), False),
    StructField("fastestLap",IntegerType(), False),
    StructField("rank",IntegerType(), False),
    StructField("fastestLapTime",StringType(), False),
    StructField("fastestLapSpeed",FloatType(), False),
    StructField("statusId",StringType(), False),
])

# COMMAND ----------

results_df = spark.read\
.schema(results_schema)\
.json(f"{raw_folder_path}/results.json")

# COMMAND ----------

results_with_columns_df = results_df.withColumnRenamed("resultId","result_id")\
                                    .withColumnRenamed("raceId","race_id")\
                                    .withColumnRenamed("driverId","driver_id")\
                                    .withColumnRenamed("constructorId","constructor_id")\
                                    .withColumnRenamed("positionText","position_text")\
                                    .withColumnRenamed("positionOrder","position_order")\
                                    .withColumnRenamed("fastestLap","fastest_lap")\
                                    .withColumnRenamed("fastestLapTime","fast_lap_time")\
                                    .withColumnRenamed("fastestLapSpeed","fastest_lap_speed")\
                                    .withColumn("data_source", lit(v_data_soruce))

# COMMAND ----------

result_drop_df = results_with_columns_df.drop(col("statusId"))

# COMMAND ----------

result_final_df = add_ingestion_date(result_drop_df)

# COMMAND ----------

result_final_df.write.mode("overwrite").partitionBy("race_id").parquet(f"{processed_folder_path}/results")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/results"))

# COMMAND ----------

dbutils.notebook.exit("Success")
