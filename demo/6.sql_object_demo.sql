-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Leson Objectives
-- MAGIC 1. Spark Sql documentacion
-- MAGIC 1. Create Database demo
-- MAGIC 1. Data tab in the UI
-- MAGIC 1. Show command
-- MAGIC 1. Describe command
-- MAGIC 1. Find the current databse

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS DEMO;

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

DESCRIBE DATABASE DEMO

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED DEMO

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df=spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

USE DEMO;
SHOW TABLES;

-- COMMAND ----------

DESC EXTENDED race_results_python

-- COMMAND ----------

SELECT *
  FROM demo.race_results_python;

-- COMMAND ----------

CREATE TABLE demo.race_results_sql
AS
SELECT *
  FROM demo.race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

DROP TABLE demo.race_results_sql;

-- COMMAND ----------

DESC EXTENDED demo.race_results_sql

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

DROP TABLE demo.race_results_sql

-- COMMAND ----------

use demo;
show tables

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # race_results_df.write.parquet("f{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # .write.parquet(f"{presentation_folder_path}/race_results_ext_py")\
-- MAGIC # .saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------


