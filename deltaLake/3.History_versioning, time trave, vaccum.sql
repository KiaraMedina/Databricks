-- Databricks notebook source
-- MAGIC %sql
-- MAGIC DESC HISTORY f1_demo.drivers_merge

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT * FROM f1_demo.drivers_merge VERSION AS OF 2

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF "2022-11-17T19:53:07.000+0000"

-- COMMAND ----------

-- df1 = spark.read.format("delta").option("timestampAsOf", '2022-11-17T19:53:07.000+0000').load("mnt/storageformula1dl/demo/drivers_merge")

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
-- MAGIC VACUUM f1_demo.drivers_merge RETAIN 0 HOURS

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF "2022-11-17T19:53:07.000+0000"

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT * FROM f1_demo.drivers_merge
