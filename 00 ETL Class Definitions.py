# Databricks notebook source
import dlt
from operations import *

# COMMAND ----------

class IngestionDLT:

    def __init__(
        self
        ,spark
        ,env_mode: str = "dev"
        ,catalog: str = "lakehouse"
        ,schema: str = "landing"
        ,volume: str = "dropbox"
    ):
        self.spark = spark
        self.env_mode = env_mode
        self.catalog = catalog
        self.schema = schema
        self.volume = volume

        use_catalog_schema(sc = self.spark, catalog = self.catalog, schema = self.schema, env_mode = self.env_mode, verbose = False)

    def __repr__(self):
        return f"""IngestionDLT(
            env_mode='{self.env_mode}'
            ,catalog='{self.catalog}'
            ,schema='{self.schema}'
            ,volume='{self.volume}')"""

    
        
    
    


# COMMAND ----------

pipeline = IngestionDLT(
    spark = spark
    ,env_mode = "dev"
    ,catalog = "lakehouse"
    ,schema = "landing"
    ,volume = 'dropbox'
)

# COMMAND ----------

display(pipeline.use_catalog_schema(verbose=True))

# COMMAND ----------

class IngestionDLT:

    def __init__(self, spark):
        self.spark = spark

    def __repr__(self):
        return f"IngestionDLT(spark={self.spark})"

    def use_catalog_schema(self, catalog, schema):
        self.spark.sql(f"use catalog {catalog}")
        self.spark.sql(f"use schema {schema}")
        return self

# COMMAND ----------

spark.sql(f"use catalog lakehouse_{env_mode}")
spark.sql("use schema landing")
display(spark.sql("select current_catalog(), current_schema()"))

# COMMAND ----------

import dlt

# COMMAND ----------

dbutils.widgets.removeAll()
