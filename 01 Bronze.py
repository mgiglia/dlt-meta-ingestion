# Databricks notebook source
dbutils.widgets.dropdown("env_mode", "dev", ("dev", "tst", "prd"), "Environment")
dbutils.widgets.text("catalog", "lakehouse", "Catalog")
dbutils.widgets.text("schema", "landing", "Schema")
dbutils.widgets.text("volume", "dropbox", "Volume")

# COMMAND ----------

env_mode = dbutils.widgets.get("env_mode")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")
print(
f"""
Notebook Variables Set:
    env_mode = {env_mode}
    catalog = {catalog}
    schema = {schema}
    volume = {volume}
"""
)

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.sql.streaming import DataStreamReader, DataStreamWriter
from typing import Callable

# COMMAND ----------

# set the catalog and schema 
def use_catalog_schema(catalog: str, schema: str, env_mode: str = "dev", verbose: bool = False):
    if env_mode == "prd":
        catalog_stmnt = f"""use catalog {catalog};"""
    else:
        catalog_stmnt = f"""use catalog {catalog}_{env_mode};"""
    
    spark.sql(catalog_stmnt)
    spark.sql(f"""use schema {schema};""")
    if verbose:
        return spark.sql("""select current_catalog(), current_schema();""")

# COMMAND ----------

def read_stream_raw(spark: SparkSession, path: str, maxFiles: int, maxBytes: str, wholeText: bool = True, options: dict = None) -> DataFrame:
    stream_schema = "value STRING"
    read_stream = (
        spark
        .readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("wholetext", wholeText)
        .option("cloudFiles.maxBytesPerTrigger", maxBytes)
        .option("cloudFiles.maxFilesPerTrigger", maxFiles)
    )

    if options is not None:
        read_stream = read_stream.options(**options)

    read_stream = (
        read_stream
        .schema(stream_schema)
        .load(path)
    )

    return read_stream

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

        use_catalog_schema(catalog = self.catalog, schema = self.schema, env_mode = self.env_mode, verbose = False)

        self.catalog_set = spark.sql("select current_catalog()").collect()[0][0]

    def __repr__(self):
        return f"""IngestionDLT(env_mode='{self.env_mode}', catalog='{self.catalog}', schema='{self.schema}', volume='{self.volume}')"""

    def ingest_raw_to_bronze(self, table_name: str, table_comment: str, table_properties: dict, source_folder_path_from_volume: str = "", maxFiles: int = 1000, maxBytes: str = "10g", wholeText: bool = True, options: dict = None):
        """
        Ingests all files in a volume's path to a key value pair bronze table.
        """
        @dlt.table(
            name = table_name
            ,comment = table_comment
            ,temporary = False
            ,table_properties = table_properties

        )
        def bronze_ingestion(spark = self.spark, source_folder_path_from_volume = source_folder_path_from_volume, maxFiles = maxFiles, maxBytes = maxBytes, wholeText = wholeText, options = options, catalog = self.catalog_set, schema = self.schema, volume = self.volume):

            if source_folder_path_from_volume == "":
                file_path = f"/Volumes/{catalog}/{schema}/{volume}/"
            else:
                file_path = f"/Volumes/{catalog}/{schema}/{volume}/{source_folder_path_from_volume}/"

            raw_df = read_stream_raw(spark = spark, path = file_path, maxFiles = maxFiles, maxBytes = maxBytes, wholeText = wholeText, options = options)

            bronze_df = (raw_df
                .withColumn("inputFilename", input_file_name())
                .select(
                    lit(file_path).alias("datasource")
                    ,"inputFileName"
                    ,current_timestamp().alias("ingestTime")
                    ,current_timestamp().cast("date").alias("ingestDate")
                    ,"value"
                )
            )

            return bronze_df

# COMMAND ----------

pipeline = IngestionDLT(
    spark = spark
    ,env_mode = env_mode
    ,catalog = "lakehouse"
    ,schema = "landing"
    ,volume = 'dropbox'
)

# COMMAND ----------

pipeline

# COMMAND ----------

display(spark.sql("select current_catalog(), current_schema()"))

# COMMAND ----------

pipeline.ingest_raw_to_bronze(
    table_name="dropbox_bronze"
    ,table_comment="A full text record of every file that has landed in our dropbox folder."
    ,table_properties=None
    ,source_folder_path_from_volume=""
)
