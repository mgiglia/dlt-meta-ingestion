# Databricks notebook source
import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run ./operations

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

# pipeline = IngestionDLT(
#     spark = spark
#     ,env_mode = "dev"
#     ,catalog = "lakehouse"
#     ,schema = "landing"
#     ,volume = 'dropbox'
# )

# COMMAND ----------

# pipeline

# COMMAND ----------

# %sql
# select current_catalog(), current_schema();

# COMMAND ----------

# spark.sql("select current_catalog()").collect()[0][0]

# COMMAND ----------

# pipeline.ingest_raw_to_bronze(
#     table_name="dropbox_bronze"
#     ,table_comment="A full text record of every file that has landed in our dropbox folder."
#     ,table_properties=None
#     ,source_folder_path_from_volume=""
# )
