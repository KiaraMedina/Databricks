# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

demo_df=race_results_df

# COMMAND ----------

display(demo_df)

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum, desc, rank

# COMMAND ----------

demo_df.select(count("*")).show()

# COMMAND ----------

demo_df.select(countDistinct("driver_name")).show()

# COMMAND ----------

demo_df.select(sum("points")).show()

# COMMAND ----------

demo_grouped_df=demo_df\
.groupBy("race_year","driver_name")\
.agg(sum("points").alias("total_points"))

# COMMAND ----------


demo_df= race_results_df.filter("race_year in (2019,2020)")

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

display(demo_grouped_df)

# COMMAND ----------

driversRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"))
demo_grouped_df.withColumn("rank", rank().over(driversRankSpec)).show(50)

# COMMAND ----------


