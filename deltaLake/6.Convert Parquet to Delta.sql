-- Databricks notebook source
-- MAGIC %sql
-- MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_convert_to_delta2(
-- MAGIC driverId INT,
-- MAGIC dob DATE,
-- MAGIC forename STRING,
-- MAGIC surname STRING,
-- MAGIC createdDate DATE,
-- MAGIC updatedDate DATE
-- MAGIC )
-- MAGIC USING PARQUET

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC INSERT INTO f1_demo.drivers_convert_to_delta2
-- MAGIC SELECT * FROM f1_demo.drivers_merge

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CONVERT TO DELTA f1_demo.drivers_convert_to_delta2

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.table("f1_demo.drivers_convert_to_delta2")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.format("parquet").save("/mnt/storageformula1dl/demo/f1_demo.drivers_convert_to_delta_new")

-- COMMAND ----------

CONVERT TO DELTA parquet.`/mnt/storageformula1dl/demo/f1_demo.drivers_convert_to_delta_new`
