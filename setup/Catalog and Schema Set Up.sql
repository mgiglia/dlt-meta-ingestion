-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.dropdown("env_mode", "dev", ("dev", "tst", "prd"), "Environment")

-- COMMAND ----------

create catalog if not exists lakehouse_${env_mode};

-- COMMAND ----------

use catalog lakehouse_${env_mode};

-- COMMAND ----------

select current_catalog();

-- COMMAND ----------

create schema if not exists landing;

-- COMMAND ----------

use schema landing;

-- COMMAND ----------

select current_schema();

-- COMMAND ----------

drop volume if exists dropbox;
create external volume dropbox
location 's3://s3-databricks-mgiglia-solutions-landing/${env_mode}/dropbox';
