# Databricks notebook source
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
