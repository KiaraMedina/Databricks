-- Databricks notebook source
-- MAGIC %sql
-- MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_txn(
-- MAGIC driverId INT,
-- MAGIC dob DATE,
-- MAGIC forename STRING,
-- MAGIC surname STRING,
-- MAGIC createdDate DATE,
-- MAGIC updatedDate DATE
-- MAGIC )
-- MAGIC USING DELTA

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC DESC HISTORY f1_demo.drivers_txn

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC INSERT INTO f1_demo.drivers_txn
-- MAGIC SELECT * FROM f1_demo.drivers_merge
-- MAGIC WHERE driverId = 1

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC DESC HISTORY f1_demo.drivers_txn
