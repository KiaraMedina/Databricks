# Databricks notebook source
dbutils.widgets.text("p_data_source","")
v_data_soruce = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date


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
.json(f"{raw_folder_path}/{v_file_date}/results.json")

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
                                    .withColumn("data_source", lit(v_data_soruce))\
                                    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

result_drop_df = results_with_columns_df.drop(col("statusId"))

# COMMAND ----------

result_final_df = add_ingestion_date(result_drop_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Method 1

# COMMAND ----------

# for race_id_list in result_final_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id}) ")
    

# COMMAND ----------



# COMMAND ----------

# result_final_df.write.mode("append").partitionBy('race_id').format("parquet").saveAsTable('f1_processed.results')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Method 2

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

result_final_df = result_final_df.select("result_id","driver_id","constructor_id","number","grid","position","position_text","position_order","points","laps","time","milliseconds","rank","fastest_lap_speed","data_source","file_date","ingestion_date","race_id")

# COMMAND ----------

if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
    result_final_df.write.mode("overwrite").insertInto("f1_processed.results")
else:
    result_final_df.write.mode("overwrite").partitionBy('race_id').format("parquet").saveAsTable('f1_processed.results')
    

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/results"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, COUNT(1)
# MAGIC from f1_processed.results
# MAGIC group by race_id
# MAGIC order by race_id DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop table f1_processed.results

# COMMAND ----------

dbutils.notebook.exit("Success")
