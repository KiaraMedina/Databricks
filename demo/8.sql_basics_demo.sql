-- Databricks notebook source
SHOW DATABASES

-- COMMAND ----------

use f1_raw

-- COMMAND ----------

show tables

-- COMMAND ----------

select *, split(name, ' ')[0] forname, split(name, ' ')[1] surname, current_timestamp()
  from constructors

-- COMMAND ----------

select *, date_format(date, 'dd-MM-yyyy')
from races

-- COMMAND ----------

select nationality, count(*) 
from drivers
group by nationality
having count(*) > 1
order by nationality

-- COMMAND ----------


