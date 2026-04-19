# Databricks notebook source
# notebooks/01_bronze.py
#
# SDP BRONZE LAYER
# ─────────────────
# Streaming table — reads every CSV in the Unity Catalog Volume using
# Auto Loader (cloudFiles), landing each row as-is with all columns as strings.
# Adds two audit columns: _source_file and _ingested_at.
#
# Uses the Spark Declarative Pipelines API (pyspark.pipelines).
# This notebook is part of the pipeline defined in spark-pipeline.yml.
# Do NOT run it interactively — attach it to the pipeline only.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

RAW_PATH = spark.conf.get(
    "raw_data_path",
    "/Volumes/citibike_catalog/citibike/raw_data",  # matches 00_ingest_raw.py
)

# Auto Loader schema evolution checkpoints must also live in a UC Volume.
# We store them alongside the raw data so everything is self-contained.
SCHEMA_LOCATION = "/Volumes/citibike_catalog/citibike/raw_data/_autoloader_schema/bronze"

# COMMAND ----------

# DBTITLE 1,Cell 3

@dp.table(
    name="bronze_trips",
    comment="Raw Citi Bike trip records — all columns retained as strings.",
)
def bronze_trips():
    """
    Streaming table (SDP @dp.table) backed by Auto Loader (cloudFiles).

    Auto Loader tracks which files have been processed via its checkpoint at
    SCHEMA_LOCATION, so only files added since the last pipeline run are
    ingested — no duplicates, no manual state management.

    inferSchema=false: every column lands as a string. Type enforcement is
    deferred to the silver layer — bronze is a fidelity layer.
    rescuedDataColumn: any unexpected column Citi Bike adds in a future month
    is captured in a JSON blob rather than silently dropped.
    """
    return (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "csv")
             .option("header", "true")
             .option("inferSchema", "false")           # all strings — bronze rule
             .option("rescuedDataColumn", "_rescued")  # catches unexpected cols
             .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
             .load(RAW_PATH)
             .withColumn("_source_file",  F.col("_metadata.file_path"))
             .withColumn("_ingested_at",  F.current_timestamp())
    )
