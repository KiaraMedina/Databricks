# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers")\
.withColumnRenamed("number","driver_number")\
.withColumnRenamed("name","driver_name")\
.withColumnRenamed("nationality","driver_nationanlity")

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")\
.withColumnRenamed("location","circuit_location")

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/race")\
.withColumnRenamed("name","race_name")\
.withColumnRenamed("race_timestamp","race_date")

# COMMAND ----------

contructor_df = spark.read.parquet(f"{processed_folder_path}/constructor")\
.withColumnRenamed("name","team")

# COMMAND ----------

result_df = spark.read.parquet(f"{processed_folder_path}/results")\
.withColumnRenamed("time","race_time")

# COMMAND ----------

race_circuit_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id,"inner")\
.select(races_df.race_id, races_df.race_year,races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

race_result_df = result_df.join(race_circuit_df, result_df.race_id == race_circuit_df.race_id)\
                          .join(drivers_df, result_df.driver_id == drivers_df.driver_id)\
                          .join(contructor_df, result_df.constructor_id == contructor_df.constructor_id )

# COMMAND ----------

final_df = race_result_df.select("race_year","race_name","race_date","circuit_location","driver_number","driver_nationanlity","team","grid","fastest_lap","race_time","points")

# COMMAND ----------

display(final_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")
