# Databricks notebook source
drivers_day1_df = spark.read \
.option("inferSchema",True)\
.option("inferSchema", True)\
.json("/mnt/storageformula1dl/raw/2021-03-28/drivers.json")\
.filter("driverId <= 10")\
.select("driverId","dob","name.forename","name.surname")

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

display(drivers_day1_df)

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day2_df = spark.read \
.option("inferSchema",True)\
.json("/mnt/storageformula1dl/raw/2021-03-28/drivers.json")\
.filter("driverId BETWEEN 6 AND 15")\
.select("driverId","dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

display(drivers_day2_df)

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day3_df = spark.read \
.option("inferSchema",True)\
.json("/mnt/storageformula1dl/raw/2021-03-28/drivers.json")\
.filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 and 20")\
.select("driverId","dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

drivers_day3_df.createOrReplaceTempView("drivers_day3")

# COMMAND ----------

display(drivers_day3_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge(
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING,
# MAGIC surname STRING,
# MAGIC createdDate DATE,
# MAGIC updateDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dia 1

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO  f1_demo.drivers_merge tgt
# MAGIC USING drivers_day1 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updateDate = CURRENT_TIMESTAMP
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename, surname, createdDate) values (driverId, dob, forename, surname, CURRENT_TIMESTAMP )
# MAGIC   

# COMMAND ----------

# MAGIC %sql SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dia 2

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO  f1_demo.drivers_merge tgt
# MAGIC USING drivers_day2 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updateDate = CURRENT_TIMESTAMP
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename, surname, createdDate) values (driverId, dob, forename, surname, CURRENT_TIMESTAMP )

# COMMAND ----------

# MAGIC %sql SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dia 3

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO  f1_demo.drivers_merge tgt
# MAGIC USING drivers_day3 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updateDate = CURRENT_TIMESTAMP
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename, surname, createdDate) values (driverId, dob, forename, surname, CURRENT_TIMESTAMP )

# COMMAND ----------

# MAGIC %sql SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# from pyspark.sql.functions import current_timestamp
# from delta.tables import *

# deltaTable = DeltaTable.forPath(spark, "/mnt/storageformula1dl/demo/drivers_merge")

# deltaTable.alias("tgt").merge(
#     drivers_day3_df.alias("upd"),
#     "tgt.driverId = upd.driverId")\
#     .whenMatchedUpdate(set = { "dob":"upd.dob", "forename":"upd.forename","surname":"upd.surname","updatedDate":"current_timestamp()"})\
#     .whenNotMatchedInsert(values =
#      {
#        "driverId": "upd.driverId",
#        "dob": "upd.dob",
#        "forename": "upd.forename",
#        "surname": "upd.surname",
#        "createdDate":"current_timestamp()"
#       }
#     )\
#     .execute()
