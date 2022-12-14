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

# def re_arrange_partition_column(input_df, partition_column):
#     column_list = []
#     for columns_name in input_df.schema.names:
#         if columns_name != partition_column:
#             column_list.append(columns_name)
#     column_list.append(partition_column)
#     output_df = input_df.select(column_list)
#     return output_df

# COMMAND ----------

# output_df = re_arrange_partition_column(result_final_df, "race_id")

# COMMAND ----------

# def overwrite_partition(input_df,db_name, table_name,partition_column):
#     output_df = re_arrange_partition_column(input_df, partition_column)
#     spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
#     if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
#         output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
#     else:
#         output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f'{db_name}.{table_name}')

# COMMAND ----------

# overwrite_partition(result_final_df,"f1_processed","results","race_id")

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(result_final_df,"f1_processed", "results",processed_folder_path,merge_condition,"race_id")

# COMMAND ----------

# def merge_delta_data(input_df,db_name, table_name,folder_path, merge_condition,partition_column):
#     spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true"
#                   )
#     from delta.tables import DeltaTable

#     if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
#         deltaTable = DeltaTable.forPath(spark,f"{folder_path}/{table_name}")
#         deltaTable.alias("tgt").merge(
#             input_df.alias("src"),
#             merge_condition)\
#         .whenMatchedUpdateAll()\
#         .whenNotMatchedInsertAll()\
#         .execute()
#     else:
#         oinput_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f'{db_name}.{table_name}')

# COMMAND ----------

# spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

# result_final_df = result_final_df.select("result_id","driver_id","constructor_id","number","grid","position","position_text","position_order","points","laps","time","milliseconds","rank","fastest_lap_speed","data_source","file_date","ingestion_date","race_id")

# COMMAND ----------

# if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#     result_final_df.write.mode("overwrite").insertInto("f1_processed.results")
# else:
#     result_final_df.write.mode("overwrite").partitionBy('race_id').format("parquet").saveAsTable('f1_processed.results')
    

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/results"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, driver_id, COUNT(1)
# MAGIC from f1_processed.results
# MAGIC group by race_id, driver_id
# MAGIC having COUNT(1) > 1
# MAGIC order by race_id, driver_id desc

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop table f1_processed.results

# COMMAND ----------

dbutils.notebook.exit("Success")
