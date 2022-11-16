# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")\
.filter("circuit_id < 70")\
.withColumnRenamed("name","circuit_name")

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/race")

# COMMAND ----------

races_filterd_df = races_df.filter("race_year = 2019 and round <= 5")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

display(races_filterd_df)

# COMMAND ----------

# inner, solo registros donde coinciden sus primaries
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner")\
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.name, races_df.round)

# COMMAND ----------

display(race_circuits_df )

# COMMAND ----------

# outer left, todos los registros de la tabla izquierda
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "left")\
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# outer rigth
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "right")\
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# full outer
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "full")\
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

#semi join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi")\
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table orders(
# MAGIC orderid int,
# MAGIC customer int,
# MAGIC order varchar(50)
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC create table customer(
# MAGIC customerid int,
# MAGIC name varchar(50))

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into orders(orderid, customer, order)values
# MAGIC (10308,2,"order 1"),
# MAGIC (10309,37,"order 2"),
# MAGIC (10310,77,"order 3")

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into customer(customerid, name)values
# MAGIC (4,"kiara"),
# MAGIC (1,"Esteban"),
# MAGIC (2,"Josselyn"),
# MAGIC (3,"Eliseo")

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from orders a inner join customer b on a.customer = b.customerid

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from orders a left join customer b on a.customer = b.customerid

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from orders a right join customer b on a.customer = b.customerid

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from orders a semi join customer b on a.customer = b.customerid

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from customer a anti join orders b on a.customerid =b.customer 

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from customer a semi join orders b on a.customerid =b.customer 

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from orders a anti join customer b on a.customer = b.customerid

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from orders a cross join customer b on a.customer = b.customerid

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from customer a cross join orders b on a.customerid =b.customer 
