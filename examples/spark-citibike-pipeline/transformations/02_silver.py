# Databricks notebook source
# notebooks/02_silver.py
#
# SDP SILVER LAYER
# ─────────────────
# Streaming table — reads bronze_trips, casts every column to its proper type,
# adds derived columns for gold aggregations, and enforces data-quality
# constraints via @dp.expect_all_or_drop (bad rows dropped and counted in
# the pipeline event log).
#
# Uses the Spark Declarative Pipelines API (pyspark.pipelines).
# This notebook is part of the pipeline defined in spark-pipeline.yml.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType

# COMMAND ----------

# Day-of-week label lookup (Spark: 1=Sunday … 7=Saturday)
DOW_LABELS = F.create_map(
    F.lit(1), F.lit("Sun"),
    F.lit(2), F.lit("Mon"),
    F.lit(3), F.lit("Tue"),
    F.lit(4), F.lit("Wed"),
    F.lit(5), F.lit("Thu"),
    F.lit(6), F.lit("Fri"),
    F.lit(7), F.lit("Sat"),
)


# COMMAND ----------

# DBTITLE 1,Cell 4

@dp.table(
    name="silver_trips",
    comment="Cleaned, typed, and enriched Citi Bike trip records.",
)
# ── Data-quality constraints ──────────────────────────────────────────────────
# @dp.expect_all_or_drop drops any row that violates at least one constraint.
# The pipeline event log records per-constraint violation counts so data quality
# can be tracked over time without inspecting the data directly.
@dp.expect_all_or_drop({
    "non_null_started_at":    "started_at IS NOT NULL",
    "non_null_ended_at":      "ended_at IS NOT NULL",
    "valid_start_lat":        "start_lat BETWEEN -90  AND  90",
    "valid_start_lng":        "start_lng BETWEEN -180 AND 180",
    "valid_end_lat":          "end_lat   BETWEEN -90  AND  90",
    "valid_end_lng":          "end_lng   BETWEEN -180 AND 180",
    "positive_duration":      "duration_seconds > 0",
    "reasonable_duration":    "duration_seconds < 86400",
    "non_null_start_station": "start_station_name IS NOT NULL",
    "non_null_end_station":   "end_station_name   IS NOT NULL",
})
def silver_trips():
    return (
        # SDP: reference a pipeline-internal streaming table with readStream.table()
        spark.readStream.table("bronze_trips")

        # ── Cast to correct types ──────────────────────────────────────────
        .select(
            F.col("ride_id"),
            F.col("rideable_type"),
            F.to_timestamp(F.col("started_at")).alias("started_at"),
            F.to_timestamp(F.col("ended_at")).alias("ended_at"),
            F.col("start_station_name"),
            F.col("start_station_id"),
            F.col("end_station_name"),
            F.col("end_station_id"),
            F.col("start_lat").cast(DoubleType()).alias("start_lat"),
            F.col("start_lng").cast(DoubleType()).alias("start_lng"),
            F.col("end_lat").cast(DoubleType()).alias("end_lat"),
            F.col("end_lng").cast(DoubleType()).alias("end_lng"),
            F.col("member_casual"),
            F.col("_source_file"),
            F.col("_ingested_at"),
        )

        # ── Derived columns ───────────────────────────────────────────────
        .withColumn(
            "duration_seconds",
            (F.unix_timestamp("ended_at") - F.unix_timestamp("started_at")).cast(LongType()),
        )
        .withColumn("start_date",  F.to_date("started_at"))
        .withColumn("start_hour",  F.hour("started_at"))
        .withColumn("end_hour",    F.hour("ended_at"))
        .withColumn("day_of_week", F.dayofweek("started_at"))      # 1=Sun … 7=Sat
        .withColumn("day_label",   DOW_LABELS[F.dayofweek("started_at")])
        .withColumn("week_start",  F.date_trunc("week", "started_at").cast("date"))
        .withColumn("is_weekend",  F.dayofweek("started_at").isin(1, 7))
    )
