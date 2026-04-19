# Databricks notebook source
# notebooks/03_gold.py
#
# SDP GOLD LAYER
# ──────────────
# Three materialized views (batch aggregations) scoped to the configured
# target station. Materialized views use @dp.materialized_view because they
# require a full scan of silver_trips on each pipeline run — not incremental
# streaming reads.
#
#   gold_station_connections  – trip counts + coordinates to/from every
#                               other station (feeds the map widget)
#   gold_hourly_counts        – departures & arrivals by day-of-week × hour
#                               (feeds the heatmap/bar charts)
#   gold_weekly_counts        – week-level departure & arrival totals
#                               (feeds the weekly trend line chart)
#
# Uses the Spark Declarative Pipelines API (pyspark.pipelines).
# This notebook is part of the pipeline defined in spark-pipeline.yml.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

TARGET_STATION = spark.conf.get("target_station", "W 21 St & 6 Ave")

# COMMAND ----------

# %md ## gold_station_connections


@dp.materialized_view(
    name="gold_station_connections",
    comment=(
        "Trip counts and average coordinates for every station connected to "
        "the target station. One row per connected station. "
        "Use for the map layer: target station ↔ all other stations."
    ),
)
def gold_station_connections():
    # SDP: reference a pipeline-internal table with spark.table()
    trips = spark.table("silver_trips")

    # ── Departures FROM target station ────────────────────────────────────
    departures = (
        trips
        .filter(F.col("start_station_name") == TARGET_STATION)
        .groupBy("end_station_name")
        .agg(
            F.count("*").alias("departure_count"),
            F.avg("end_lat").alias("other_lat"),
            F.avg("end_lng").alias("other_lng"),
        )
        .withColumnRenamed("end_station_name", "other_station")
    )

    # ── Arrivals AT target station ────────────────────────────────────────
    arrivals = (
        trips
        .filter(F.col("end_station_name") == TARGET_STATION)
        .groupBy("start_station_name")
        .agg(
            F.count("*").alias("arrival_count"),
            F.avg("start_lat").alias("other_lat"),
            F.avg("start_lng").alias("other_lng"),
        )
        .withColumnRenamed("start_station_name", "other_station")
    )

    # Full outer join: keep stations that only appear as departure or only
    # as arrival destinations — a zero in the other direction is still useful.
    joined = (
        departures.alias("d")
        .join(arrivals.alias("a"), on="other_station", how="outer")
        .select(
            F.coalesce(F.col("d.other_station"), F.col("a.other_station")).alias("other_station"),
            F.coalesce(F.col("d.other_lat"),     F.col("a.other_lat")).alias("lat"),
            F.coalesce(F.col("d.other_lng"),     F.col("a.other_lng")).alias("lng"),
            F.coalesce(F.col("d.departure_count"), F.lit(0)).alias("departure_count"),
            F.coalesce(F.col("a.arrival_count"),   F.lit(0)).alias("arrival_count"),
        )
        .withColumn("total_trips",      F.col("departure_count") + F.col("arrival_count"))
        .withColumn("net_flow",         F.col("departure_count") - F.col("arrival_count"))
        .withColumn("target_station",   F.lit(TARGET_STATION))
    )

    # Also add the target station's own coordinates so the map can pin it
    target_coords = (
        trips
        .filter(F.col("start_station_name") == TARGET_STATION)
        .agg(F.avg("start_lat").alias("t_lat"), F.avg("start_lng").alias("t_lng"))
    )

    return joined.crossJoin(target_coords)


# COMMAND ----------

# %md ## gold_hourly_counts


@dp.materialized_view(
    name="gold_hourly_counts",
    comment=(
        "Departure and arrival counts at the target station, "
        "broken down by day-of-week (1=Sun…7=Sat) and hour-of-day (0–23). "
        "Use for heatmap and grouped bar charts."
    ),
)
def gold_hourly_counts():
    trips = spark.table("silver_trips")

    departures = (
        trips
        .filter(F.col("start_station_name") == TARGET_STATION)
        .groupBy("day_of_week", "day_label", "start_hour")
        .agg(F.count("*").alias("departure_count"))
    )

    arrivals = (
        trips
        .filter(F.col("end_station_name") == TARGET_STATION)
        .groupBy(
            F.dayofweek("ended_at").alias("day_of_week"),
            F.date_format("ended_at", "EEE").alias("day_label"),
            F.hour("ended_at").alias("start_hour"),
        )
        .agg(F.count("*").alias("arrival_count"))
    )

    return (
        departures.alias("d")
        .join(arrivals.alias("a"), on=["day_of_week", "start_hour"], how="outer")
        .select(
            F.coalesce(F.col("d.day_of_week"), F.col("a.day_of_week")).alias("day_of_week"),
            F.coalesce(F.col("d.day_label"),   F.col("a.day_label")).alias("day_label"),
            F.coalesce(F.col("d.start_hour"),  F.col("a.start_hour")).alias("hour"),
            F.coalesce(F.col("d.departure_count"), F.lit(0)).alias("departure_count"),
            F.coalesce(F.col("a.arrival_count"),   F.lit(0)).alias("arrival_count"),
        )
        .withColumn("total_count",    F.col("departure_count") + F.col("arrival_count"))
        .withColumn("target_station", F.lit(TARGET_STATION))
        .orderBy("day_of_week", "hour")
    )


# COMMAND ----------

# %md ## gold_weekly_counts


@dp.materialized_view(
    name="gold_weekly_counts",
    comment=(
        "Weekly departure and arrival totals at the target station. "
        "week_start is the Monday of each ISO week. "
        "Use for the weekly trend line chart."
    ),
)
def gold_weekly_counts():
    trips = spark.table("silver_trips")

    departures = (
        trips
        .filter(F.col("start_station_name") == TARGET_STATION)
        .groupBy("week_start")
        .agg(F.count("*").alias("departures"))
    )

    arrivals = (
        trips
        .filter(F.col("end_station_name") == TARGET_STATION)
        .groupBy(F.date_trunc("week", "ended_at").cast("date").alias("week_start"))
        .agg(F.count("*").alias("arrivals"))
    )

    return (
        departures.alias("d")
        .join(arrivals.alias("a"), on="week_start", how="outer")
        .select(
            F.coalesce(F.col("d.week_start"), F.col("a.week_start")).alias("week_start"),
            F.coalesce(F.col("d.departures"), F.lit(0)).alias("departures"),
            F.coalesce(F.col("a.arrivals"),   F.lit(0)).alias("arrivals"),
        )
        .withColumn("total_trips",    F.col("departures") + F.col("arrivals"))
        .withColumn("target_station", F.lit(TARGET_STATION))
        .orderBy("week_start")
    )
