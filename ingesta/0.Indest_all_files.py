# Databricks notebook source
v_result = dbutils.notebook.run("1.Ingest circuits.csv file",0,{"p_data_source":"Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("2.Ingest races.csv file",0,{"p_data_source":"Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("3.Ingest constructor.json file",0,{"p_data_source":"Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("4.ingest_drivers_file",0,{"p_data_source":"Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("5.ingest_result.csv file",0,{"p_data_source":"Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("6.Ingest pit_stops.json file",0,{"p_data_source":"Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("7.Ingest_lap_times_file",0,{"p_data_source":"Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("8.Ingest_qualifying_file",0,{"p_data_source":"Ergast API"})

# COMMAND ----------

v_result
