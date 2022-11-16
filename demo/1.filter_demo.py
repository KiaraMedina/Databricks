# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/race").filter("race_year = 2019")

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

races_filterd_df = races_df.filter("race_year = 2019")

# COMMAND ----------

display(races_filterd_df)

# COMMAND ----------

races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner" )\
.select(circuits_df.name, circuits_df.location, circuits_df.country, races_df.round)

# COMMAND ----------

display(races_circuits_df)
