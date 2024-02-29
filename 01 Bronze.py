# Databricks notebook source
dbutils.widgets.dropdown("env_mode", "dev", ("dev", "tst", "prd"), "Environment")

# COMMAND ----------

env_mode = dbutils.widgets.get("env_mode")
