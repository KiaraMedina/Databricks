-- Databricks notebook source

CREATE TEMP VIEW v_race_results
AS
SELECT *
  FROM demo.race_results_python
WHERE race_year = 2018;

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results
AS
SELECT *
  FROM demo.race_results_python
WHERE race_year = 2018;

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

show tables in global_temp
