# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # Delta Lake Management and Streaming
# MAGIC
# MAGIC ## Business Context
# MAGIC
# MAGIC Welcome to the **NYC Yellow Taxi Analytics Platform**! As a Data Engineer at MetroFleet Analytics, you're supporting NYC's largest taxi dispatch network. Your fleet processes thousands of rides daily across 200+ zones, and operations managers need reliable, real-time insights into:
# MAGIC
# MAGIC - **Zone Performance**: Which pickup/dropoff zones generate the most revenue?
# MAGIC - **Fare Trends**: How do trip distances and fares vary throughout the day?
# MAGIC - **Data Quality**: Managing corrections to fare data (cancellations, adjustments, duplicate trips)
# MAGIC - **Real-Time Operations**: Streaming new trip completions as they happen
# MAGIC
# MAGIC Previously, your data warehouse ran batch updates every 6 hours, causing stale analytics and missed optimization opportunities. You'll build a modern **Delta Lake pipeline** that enables efficient data management, optimized query performance, and real-time ingestion.
# MAGIC
# MAGIC ## Dataset Overview
# MAGIC
# MAGIC You'll work with NYC Yellow Taxi trip data from `samples.nyctaxi.trips`:
# MAGIC
# MAGIC | Column | Type | Description | Usage |
# MAGIC |--------|------|-------------|-------|
# MAGIC | `tpep_pickup_datetime` | timestamp | When passenger was picked up | Time-based analytics, windowing |
# MAGIC | `tpep_dropoff_datetime` | timestamp | When passenger was dropped off | Trip duration calculation |
# MAGIC | `trip_distance` | double | Trip distance in miles | Revenue per mile analysis |
# MAGIC | `fare_amount` | double | Fare charged in USD | Revenue aggregations |
# MAGIC | `pickup_zip` | int | Pickup location ZIP code | Zone performance, Z-order optimization |
# MAGIC | `dropoff_zip` | int | Dropoff location ZIP code | Route analysis |
# MAGIC
# MAGIC **Sample Size**: ~100,000 trips from NYC Yellow Taxi dataset
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC This comprehensive lab covers three core Delta Lake topics compatible with Databricks Free Edition:
# MAGIC
# MAGIC 1. **Delta Lake Foundations** - Write to Delta format, schema evolution, time travel, table history
# MAGIC 2. **Data Management Operations** - CRUD operations (INSERT/UPDATE/DELETE), MERGE upserts, OPTIMIZE with Z-ordering, VACUUM
# MAGIC 3. **Streaming Pipeline with Auto Loader** - Incremental file ingestion, structured streaming, windowed aggregations, streaming MERGE
# MAGIC
# MAGIC **Note**: This lab uses `trigger(availableNow=True)` for batch-style processing compatible with Databricks Free Edition serverless compute. The Delta Lake and Structured Streaming APIs you'll learn are identical to production environments!
# MAGIC
# MAGIC ## Lab Journey
# MAGIC
# MAGIC **Act 1: Establish the Data Lake** → Convert raw taxi data to Delta format with proper schema management
# MAGIC **Act 2: Manage Data Quality** → Handle corrections, deletions, and incremental updates efficiently
# MAGIC **Act 3: Real-Time Ingestion** → Build Bronze → Silver → Gold streaming pipeline with Auto Loader
# MAGIC
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a catalog and schema for our Delta Lake lab
# MAGIC CREATE CATALOG IF NOT EXISTS nyctaxi_catalog;
# MAGIC
# MAGIC -- Create a schema (database) in the catalog
# MAGIC CREATE SCHEMA IF NOT EXISTS nyctaxi_catalog.analytics;
# MAGIC
# MAGIC -- Create a managed volume for file storage
# MAGIC CREATE VOLUME IF NOT EXISTS nyctaxi_catalog.analytics.workspace;

# COMMAND ----------

# Set up working directory using Unity Catalog volume
import os

# Use Unity Catalog managed volume for file storage
working_dir = "/Volumes/nyctaxi_catalog/analytics/workspace"
checkpoint_dir = f"{working_dir}/checkpoints"

print(f"Working directory: {working_dir}")
print(f"Checkpoint directory: {checkpoint_dir}")

# COMMAND ----------

# Clean up working directory to account for any failed previous runs.
# IMPORTANT: Stop any active streaming queries first
for query in spark.streams.active:
    print(f"Stopping query: {query.id}")
    query.stop()

# Now clean up directories
try:
    dbutils.fs.rm(f"{working_dir}", recurse=True)
    print(f"✅ Cleaned up working directory: {working_dir}")
except Exception as e:
    print(f"⚠️ Cleanup note: {e}")
    print("Continuing with lab...")

# Recreate working directory structure
dbutils.fs.mkdirs(working_dir)
dbutils.fs.mkdirs(checkpoint_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Important: Databricks Free Edition Compatibility
# MAGIC
# MAGIC This lab is designed for **Databricks Free Edition** with serverless compute, which has specific limitations:
# MAGIC
# MAGIC ## Streaming Limitations:
# MAGIC - ❌ **Cannot use `display()` on streaming DataFrames** - Interactive streaming display not supported
# MAGIC - ❌ **No continuous streaming** - Cannot use default or time-based triggers
# MAGIC - ❌ **"update" output mode not supported** - Use "append" for windowed aggregations and "complete" for non-windowed aggregations
# MAGIC - ✅ **Use `trigger(availableNow=True)`** - Processes all available data in batch mode
# MAGIC - ✅ **Use watermarks with aggregations** - Required for append mode to work with windowed aggregations
# MAGIC - ✅ **Verify outputs** - Read streaming results as batch DataFrames for verification
# MAGIC
# MAGIC ## Delta Lake Features:
# MAGIC - ✅ **All core Delta Lake features supported** - CRUD, MERGE, OPTIMIZE, Z-order, VACUUM, time travel
# MAGIC - ✅ **Auto Loader (cloudFiles) supported** - Schema inference and incremental ingestion
# MAGIC
# MAGIC ## Pattern Used Throughout This Lab:
# MAGIC
# MAGIC 1. **Create streaming DataFrame** with `readStream`
# MAGIC 2. **Apply transformations** (filters, aggregations, joins)
# MAGIC 3. **Write with `writeStream`** using `trigger(availableNow=True)`
# MAGIC 4. **Verify by reading output** as batch DataFrame and using `display()`
# MAGIC
# MAGIC This approach simulates real-time streaming while working within Free Edition constraints. You're learning the exact same APIs used in production!

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 1: Delta Lake Foundations
# MAGIC
# MAGIC **Business Goal:** Establish a reliable Delta Lake table with proper schema management and versioning for historical trip analysis.
# MAGIC
# MAGIC In this section, you'll learn how to work with Delta Lake tables: writing data in Delta format, evolving schemas to add new columns, handling breaking schema changes, and using time travel to query historical versions.
# MAGIC
# MAGIC ## Key Concepts:
# MAGIC - **Delta Format**: ACID-compliant storage format built on Parquet
# MAGIC - **Schema Evolution**: Adding new columns without rewriting entire dataset
# MAGIC - **Schema Enforcement**: Preventing incompatible data types
# MAGIC - **overwriteSchema**: Enabling breaking schema changes (type modifications)
# MAGIC - **Time Travel**: Querying previous versions of the table
# MAGIC - **Table History**: Audit trail of all table changes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.1: Write Initial Delta Table
# MAGIC
# MAGIC Create your baseline trip history by converting NYC Yellow Taxi data from the `samples` catalog into Delta format. Delta Lake provides ACID transactions, scalable metadata handling, and time travel capabilities built on top of Parquet files.
# MAGIC
# MAGIC **Why Delta?** Traditional Parquet files don't support updates, deletes, or schema evolution. Delta Lake adds these critical features while maintaining compatibility with existing Spark DataFrames.

# COMMAND ----------

# TODO: Load NYC taxi trips from samples catalog and write to Delta format
# Use spark.table() to load, then write with Delta format

from pyspark.sql.functions import col

# Load taxi trips
trips_df = spark.table("samples.nyctaxi.trips").select("tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "fare_amount", "pickup_zip", "dropoff_zip")  # Table name from samples catalog

print(f"Loaded {trips_df.count():,} taxi trips")
display(trips_df.limit(10))

# Write to Delta format
(trips_df
 .write
 .format("delta")  # Delta format
 .mode("overwrite")  # Write mode
 .save(f"{working_dir}/taxi_trips_delta")
)

print(f"✅ Written trips to Delta format at {working_dir}/taxi_trips_delta")

# COMMAND ----------

# CHECK YOUR WORK
delta_df = spark.read.format("delta").load(f"{working_dir}/taxi_trips_delta")
assert delta_df.count() > 0, "Delta table should contain data"
assert set(delta_df.columns) == {"tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "fare_amount", "pickup_zip", "dropoff_zip"}, "Should have correct columns"
print("✅ Task 1.1 complete: Initial Delta table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.2: Schema Evolution - Add Calculated Columns
# MAGIC
# MAGIC Operations managers need additional metrics: trip duration (minutes) and average speed (mph). Add these as calculated columns using **schema evolution**.
# MAGIC
# MAGIC **Challenge**: By default, Delta Lake enforces schema-on-write - new columns in appended data will cause errors. You'll need to enable schema evolution with `.option("mergeSchema", "true")`.

# COMMAND ----------

# TODO: Add trip_duration_minutes and avg_speed_mph columns, then append with schema merge
# Calculate duration: (dropoff_time - pickup_time) in minutes
# Calculate speed: trip_distance / (duration_in_hours)

from pyspark.sql.functions import unix_timestamp, round, try_divide

# Read existing Delta table
base_df = spark.read.format("delta").load(f"{working_dir}/taxi_trips_delta")

# Add calculated columns
enhanced_df = base_df.withColumn(
    "trip_duration_minutes",
    round((unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 60, 2)  # Dropoff and pickup columns
).withColumn(
    "avg_speed_mph",
    round(try_divide(col("trip_distance"), col("trip_duration_minutes") / 60), 2)  # Distance and duration columns
)

print("Enhanced schema:")
enhanced_df.printSchema()

# Try appending without mergeSchema (will fail!)
try:
    enhanced_df.write.format("delta").mode("append").save(f"{working_dir}/taxi_trips_delta")
    print("❌ This shouldn't succeed without mergeSchema!")
except Exception as e:
    print(f"✅ Expected error: {str(e)[:100]}...")

# Now enable schema evolution
(enhanced_df
 .write
 .format("delta")  # Delta format
 .mode("append")  # Append mode
 .option("mergeSchema", "true")  # Option name and value for schema merging
 .save(f"{working_dir}/taxi_trips_delta")
)

print("✅ Schema evolution successful - new columns added")

# COMMAND ----------

# CHECK YOUR WORK
evolved_df = spark.read.format("delta").load(f"{working_dir}/taxi_trips_delta")
assert "trip_duration_minutes" in evolved_df.columns, "Should have trip_duration_minutes column"
assert "avg_speed_mph" in evolved_df.columns, "Should have avg_speed_mph column"
assert evolved_df.count() >= base_df.count(), "Row count should be at least as large as the original"
print("✅ Task 1.2 complete: Schema evolution with calculated columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.3: Overwrite with Breaking Schema Change
# MAGIC
# MAGIC The finance department requires `fare_amount` as `DECIMAL(10,2)` instead of `DOUBLE` for accounting precision. This is a **breaking schema change** - simply adding a column is safe, but changing a column's data type requires `overwriteSchema`.
# MAGIC
# MAGIC **Warning**: `overwriteSchema=true` replaces the entire table schema. Use carefully!

# COMMAND ----------

# TODO: Cast fare_amount to DECIMAL(10,2) and overwrite with schema change
# This demonstrates a breaking schema change that requires overwriteSchema

from pyspark.sql.types import DecimalType

# Read current data (with duplicates from previous append)
current_df = spark.read.format("delta").load(f"{working_dir}/taxi_trips_delta")

# Remove duplicates and cast fare_amount
transformed_df = (current_df
    .dropDuplicates(["tpep_pickup_datetime", "pickup_zip", "dropoff_zip"])
    .withColumn("fare_amount", col("fare_amount").cast(DecimalType(10,2)))  # Cast to DecimalType
)

print("Transformed schema:")
transformed_df.printSchema()

# Overwrite with schema change
(transformed_df
 .write
 .format("delta")  # Delta format
 .mode("overwrite")  # Overwrite mode
 .option("overwriteSchema", "true")  # Option for schema overwrite
 .save(f"{working_dir}/taxi_trips_delta")
)

print("✅ Schema overwrite complete - fare_amount is now DECIMAL(10,2)")

# COMMAND ----------

# CHECK YOUR WORK
final_df = spark.read.format("delta").load(f"{working_dir}/taxi_trips_delta")
fare_field = [f for f in final_df.schema.fields if f.name == "fare_amount"][0]
assert "decimal" in str(fare_field.dataType).lower(), "fare_amount should be DecimalType"
assert final_df.count() == trips_df.count(), "Should have original row count (duplicates removed)"
print("✅ Task 1.3 complete: Breaking schema change applied")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.4: Time Travel and Table History
# MAGIC
# MAGIC One of Delta Lake's most powerful features is **time travel** - the ability to query previous versions of your table. Every write operation creates a new version, and Delta Lake maintains a transaction log so you can:
# MAGIC - Audit data changes
# MAGIC - Rollback to previous versions
# MAGIC - Reproduce ML training datasets
# MAGIC - Debug data quality issues
# MAGIC
# MAGIC Query your table's history and compare Version 0 (original) vs Version 2 (final) to see how the data evolved.

# COMMAND ----------

# MAGIC %md
# MAGIC **Use SQL for table history inspection:**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: View complete table history
# MAGIC -- Replace with the path to your Delta table
# MAGIC
# MAGIC DESCRIBE HISTORY delta.`/Volumes/nyctaxi_catalog/analytics/workspace/taxi_trips_delta`

# COMMAND ----------

# MAGIC %md
# MAGIC **Query specific versions:**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Query Version 0 (original data, no calculated columns)
# MAGIC -- Use VERSION AS OF to query a specific version
# MAGIC SELECT * FROM delta.`/Volumes/nyctaxi_catalog/analytics/workspace/taxi_trips_delta` VERSION AS OF 0 LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Query current version (with calculated columns and DECIMAL fare_amount)
# MAGIC SELECT * FROM delta.`/Volumes/nyctaxi_catalog/analytics/workspace/taxi_trips_delta` LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Compare average fare between Version 0 and current
# MAGIC -- Query Version 0 and current version to compare metrics
# MAGIC SELECT
# MAGIC     'Version 0' AS version,
# MAGIC     COUNT(*) AS trip_count,
# MAGIC     AVG(fare_amount) AS avg_fare
# MAGIC FROM delta.`/Volumes/nyctaxi_catalog/analytics/workspace/taxi_trips_delta` VERSION AS OF 0
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'Current' AS version,
# MAGIC     COUNT(*) AS trip_count,
# MAGIC     AVG(CAST(fare_amount AS DOUBLE)) AS avg_fare
# MAGIC FROM delta.`/Volumes/nyctaxi_catalog/analytics/workspace/taxi_trips_delta`

# COMMAND ----------

# CHECK YOUR WORK
# Verify we can read Version 0
version_0_df = spark.read.format("delta").option("versionAsOf", 0).load(f"{working_dir}/taxi_trips_delta")
assert "trip_duration_minutes" not in version_0_df.columns, "Version 0 should not have calculated columns"
print("✅ Task 1.4 complete: Time travel and table history explored")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 2: Data Management Operations
# MAGIC
# MAGIC **Business Goal:** Handle real-world data corrections, deletions, and incremental updates efficiently using Delta Lake's CRUD operations.
# MAGIC
# MAGIC In production, data is never static. You need to:
# MAGIC - **INSERT** new batches of trips as they arrive
# MAGIC - **UPDATE** fares when surge pricing is applied retroactively
# MAGIC - **DELETE** cancelled or fraudulent trips
# MAGIC - **MERGE** daily corrections (fare adjustments, duplicate resolution)
# MAGIC - **OPTIMIZE** storage with file compaction and Z-ordering
# MAGIC - **VACUUM** to reclaim storage from old versions
# MAGIC
# MAGIC ## Key Concepts:
# MAGIC - **ACID Transactions**: All operations are atomic, consistent, isolated, durable
# MAGIC - **MERGE (Upsert)**: Update existing records or insert new ones in a single operation
# MAGIC - **OPTIMIZE**: Compact small files into larger ones for better query performance
# MAGIC - **Z-Order**: Collocate related data in the same files (e.g., group by pickup_zip)
# MAGIC - **VACUUM**: Remove old data files no longer referenced by current table version

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.1: UPDATE - Apply Surge Pricing
# MAGIC
# MAGIC During evening rush hour (5PM-7PM), pickup zone 10001 (Midtown Manhattan) had high demand. Apply a retroactive 10% surge pricing adjustment to these trips.
# MAGIC
# MAGIC **Business Impact**: Operations can now correct pricing after analyzing demand patterns, ensuring fair compensation for drivers during peak periods.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Update fare_amount for surge pricing
# MAGIC -- Conditions: pickup_zip = 10001 AND hour between 17-19 (5PM-7PM)
# MAGIC -- Action: Increase fare by 10% (fare * 1.10)
# MAGIC
# MAGIC UPDATE delta.`/Volumes/nyctaxi_catalog/analytics/workspace/taxi_trips_delta`
# MAGIC SET fare_amount = CAST(fare_amount * 1.10 AS DECIMAL(10,2))
# MAGIC WHERE pickup_zip = 10001 AND hour(tpep_pickup_datetime) >= 17 AND hour(tpep_pickup_datetime) < 19

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify the update
# MAGIC SELECT pickup_zip, hour(tpep_pickup_datetime) AS pickup_hour,
# MAGIC        COUNT(*) AS trips, AVG(CAST(fare_amount AS DOUBLE)) AS avg_fare
# MAGIC FROM delta.`/Volumes/nyctaxi_catalog/analytics/workspace/taxi_trips_delta`
# MAGIC WHERE pickup_zip = 10001
# MAGIC GROUP BY pickup_zip, pickup_hour
# MAGIC ORDER BY pickup_hour;

# COMMAND ----------

# CHECK YOUR WORK
updated_trips = spark.sql("""
    SELECT COUNT(*) AS cnt FROM delta.`/Volumes/nyctaxi_catalog/analytics/workspace/taxi_trips_delta`
    WHERE pickup_zip = 10001 AND hour(tpep_pickup_datetime) BETWEEN 17 AND 19
""").collect()[0]["cnt"]
print(f"Updated {updated_trips} trips with surge pricing")
print("✅ Task 2.1 complete: Surge pricing applied")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.2: DELETE - Remove Invalid Trips
# MAGIC
# MAGIC Data quality audit revealed trips with invalid values:
# MAGIC - Zero or negative fares (payment processing errors)
# MAGIC - Negative trip distances (GPS errors)
# MAGIC - Zero or negative trip duration (timestamp errors)
# MAGIC
# MAGIC Remove these records to ensure analytics accuracy.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Count invalid trips before deletion
# MAGIC SELECT 'Invalid Fares' AS issue, COUNT(*) AS count
# MAGIC FROM delta.`/Volumes/nyctaxi_catalog/analytics/workspace/taxi_trips_delta`
# MAGIC WHERE fare_amount <= 0
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'Invalid Distance' AS issue, COUNT(*) AS count
# MAGIC FROM delta.`/Volumes/nyctaxi_catalog/analytics/workspace/taxi_trips_delta`
# MAGIC WHERE trip_distance < 0
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'Invalid Duration' AS issue, COUNT(*) AS count
# MAGIC FROM delta.`/Volumes/nyctaxi_catalog/analytics/workspace/taxi_trips_delta`
# MAGIC WHERE trip_duration_minutes <= 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Delete invalid trips
# MAGIC -- Conditions: fare_amount <= 0 OR trip_distance < 0 OR trip_duration_minutes <= 0
# MAGIC
# MAGIC DELETE FROM delta.`/Volumes/nyctaxi_catalog/analytics/workspace/taxi_trips_delta`
# MAGIC WHERE fare_amount <= 0 OR trip_distance < 0 OR trip_duration_minutes <= 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify deletions
# MAGIC SELECT COUNT(*) AS remaining_trips FROM delta.`/Volumes/nyctaxi_catalog/analytics/workspace/taxi_trips_delta`;

# COMMAND ----------

# CHECK YOUR WORK
remaining_df = spark.read.format("delta").load(f"{working_dir}/taxi_trips_delta")
assert remaining_df.filter("fare_amount <= 0").count() == 0, "Should have no invalid fares"
assert remaining_df.filter("trip_distance < 0").count() == 0, "Should have no invalid distances"
print("✅ Task 2.2 complete: Invalid trips removed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.3: MERGE - Daily Batch Corrections
# MAGIC
# MAGIC Each night, the billing system sends a corrections file with:
# MAGIC - **Fare adjustments** (complaints, promotions, refunds)
# MAGIC - **New trips** that were delayed in processing
# MAGIC
# MAGIC Use **MERGE** to apply these updates efficiently in a single operation (upsert = update + insert).

# COMMAND ----------

# TODO
# Create corrections dataset: apply 5% discount to high-fare trips in zone 10001
corrections_df = spark.sql("""
    SELECT
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        trip_distance,
        CAST(fare_amount * 0.95 AS DECIMAL(10,2)) AS fare_amount,  -- 5% discount
        pickup_zip,
        dropoff_zip,
        trip_duration_minutes,
        avg_speed_mph
    FROM delta.`/Volumes/nyctaxi_catalog/analytics/workspace/taxi_trips_delta`
    WHERE pickup_zip = 10001 AND fare_amount > 50
    LIMIT 100
""")

# Also add some "new" trips by modifying timestamps
from pyspark.sql.functions import expr, lit

new_trips_df = spark.sql("""
    SELECT
        tpep_pickup_datetime + INTERVAL 1 DAY AS tpep_pickup_datetime,
        tpep_dropoff_datetime + INTERVAL 1 DAY AS tpep_dropoff_datetime,
        trip_distance,
        CAST(fare_amount AS DECIMAL(10,2)) AS fare_amount,
        pickup_zip,
        dropoff_zip,
        trip_duration_minutes,
        avg_speed_mph
    FROM delta.`/Volumes/nyctaxi_catalog/analytics/workspace/taxi_trips_delta`
    WHERE pickup_zip = 10002
    LIMIT 50
""")

# Combine corrections and new trips
all_corrections_df = corrections_df.union(new_trips_df)

print(f"Corrections: {corrections_df.count()} fare adjustments + {new_trips_df.count()} new trips")

# Register as temp view for MERGE
all_corrections_df.createOrReplaceTempView("trip_corrections")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: MERGE corrections into main table
# MAGIC -- Match on: pickup/dropoff times and pickup_zip
# MAGIC -- When matched: UPDATE fare_amount
# MAGIC -- When not matched: INSERT all columns
# MAGIC
# MAGIC MERGE INTO delta.`/Volumes/nyctaxi_catalog/analytics/workspace/taxi_trips_delta` AS target
# MAGIC USING trip_corrections AS source
# MAGIC ON target.tpep_pickup_datetime = source.tpep_pickup_datetime
# MAGIC AND target.pickup_zip = source.pickup_zip
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET fare_amount = source.fare_amount
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify MERGE results
# MAGIC SELECT COUNT(*) AS total_trips FROM delta.`/Volumes/nyctaxi_catalog/analytics/workspace/taxi_trips_delta`;

# COMMAND ----------

# CHECK YOUR WORK
print("✅ Task 2.3 complete: Daily corrections merged")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.4: OPTIMIZE with Z-Order
# MAGIC
# MAGIC Over time, many small files accumulate (each write creates new Parquet files). This degrades query performance. **OPTIMIZE** compacts small files into larger ones.
# MAGIC
# MAGIC Additionally, use **Z-ordering** on `pickup_zip` to collocate data from the same zones together. This dramatically speeds up queries that filter by zone (e.g., "show me all trips from zone 10001").
# MAGIC
# MAGIC **Z-Order Intuition**: Instead of random file layout, Z-order clusters related data. Queries filtering by pickup_zip can skip entire files!

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check current file count
# MAGIC DESCRIBE DETAIL delta.`/Volumes/nyctaxi_catalog/analytics/workspace/taxi_trips_delta`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Compact files and Z-order by pickup_zip
# MAGIC
# MAGIC OPTIMIZE delta.`/Volumes/nyctaxi_catalog/analytics/workspace/taxi_trips_delta`
# MAGIC ZORDER BY (pickup_zip);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check file count after optimization
# MAGIC DESCRIBE DETAIL delta.`/Volumes/nyctaxi_catalog/analytics/workspace/taxi_trips_delta`;

# COMMAND ----------

# CHECK YOUR WORK
print("✅ Task 2.4 complete: Table optimized with Z-ordering")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.5: VACUUM - Reclaim Storage
# MAGIC
# MAGIC OPTIMIZE created new compacted files, but the old small files still exist (for time travel). **VACUUM** permanently deletes files not referenced by the current table version.
# MAGIC
# MAGIC **Note**: VACUUM uses a default retention period of 7 days. Files older than this period that are no longer referenced will be deleted. In production, you can specify custom retention periods like `RETAIN 168 HOURS`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Preview files that will be deleted (DRY RUN)
# MAGIC -- Note: 168 hours (7 days) is the minimum retention period
# MAGIC
# MAGIC VACUUM delta.`/Volumes/nyctaxi_catalog/analytics/workspace/taxi_trips_delta` RETAIN 168 HOURS DRY RUN;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Execute vacuum (permanently delete old files)
# MAGIC -- Note: Files older than 168 hours (7 days) will be deleted
# MAGIC
# MAGIC VACUUM delta.`/Volumes/nyctaxi_catalog/analytics/workspace/taxi_trips_delta` RETAIN 168 HOURS;

# COMMAND ----------

# CHECK YOUR WORK
print("✅ Task 2.5 complete: Old data files vacuumed")
print("⚠️  Note: Files older than the retention period are permanently deleted")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 3: Streaming Pipeline with Auto Loader
# MAGIC
# MAGIC **Business Goal:** Build a real-time trip ingestion pipeline using Auto Loader and Structured Streaming to process new trip files as they arrive.
# MAGIC
# MAGIC Modern data architectures use a **Medallion Architecture**:
# MAGIC - **Bronze**: Raw ingestion (Auto Loader) - no transformations, just load files incrementally
# MAGIC - **Silver**: Cleaned and aggregated data - business logic applied
# MAGIC - **Gold**: Business-level aggregates - ready for analytics and BI tools
# MAGIC
# MAGIC You'll build all three layers with NYC taxi trip data.
# MAGIC
# MAGIC ## Key Concepts:
# MAGIC - **Auto Loader (cloudFiles)**: Incrementally load new files with automatic schema inference
# MAGIC - **Structured Streaming**: Process data as unbounded tables
# MAGIC - **Watermarks**: Handle late-arriving data in windowed aggregations
# MAGIC - **Window Functions**: Group data into time buckets (hourly, daily)
# MAGIC - **foreachBatch**: Apply complex operations (MERGE) in streaming pipelines
# MAGIC - **Checkpointing**: Track processed data for fault tolerance

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.1: Simulate Streaming Source
# MAGIC
# MAGIC In production, trip data arrives as JSON files uploaded hourly from taxi meters. Simulate this by exporting sample trips as 10 JSON file batches (representing 10 hours of data).

# COMMAND ----------

# TODO
# Export sample trips as JSON files (10 batches simulating hourly uploads)

# Get sample trips
sample_trips_df = spark.table("samples.nyctaxi.trips").limit(1000)

# Split into 10 batches and write as JSON
for i in range(10):
    batch_df = sample_trips_df.filter(f"hash(tpep_pickup_datetime) % 10 = {i}")
    (batch_df
     .write
     .format("json")  # JSON format
     .mode("overwrite")  # Overwrite mode
     .save(f"{working_dir}/streaming_source/batch_{i}")
    )

print(f"✅ Created 10 JSON batches simulating hourly taxi meter uploads")
print(f"Source location: {working_dir}/streaming_source")

# COMMAND ----------

# CHECK YOUR WORK
source_files = dbutils.fs.ls(f"{working_dir}/streaming_source")
assert len(source_files) == 10, "Should have 10 batches"
print("✅ Task 3.1 complete: Streaming source simulated")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.2: Auto Loader - Bronze Layer
# MAGIC
# MAGIC Configure **Auto Loader** to incrementally ingest JSON files. Auto Loader:
# MAGIC - Automatically infers schema from JSON
# MAGIC - Tracks processed files (no duplicates)
# MAGIC - Handles schema evolution automatically
# MAGIC - Scales to millions of files
# MAGIC
# MAGIC **Free Edition Note**: We use `trigger(availableNow=True)` instead of continuous processing. This processes all available files then stops (batch-style).

# COMMAND ----------

# TODO: Configure Auto Loader with schema inference
# Format: cloudFiles (Databricks Auto Loader)
# cloudFiles.format: json
# cloudFiles.schemaLocation: checkpoint location for inferred schema

bronze_stream_df = (spark.readStream
    .format("cloudFiles")  # CloudFiles format
    .option("cloudFiles.format", "json")  # Source file format
    .option("cloudFiles.schemaLocation", f"{checkpoint_dir}/bronze_schema")
    .load(f"{working_dir}/streaming_source")
)

print("Bronze stream schema:")
bronze_stream_df.printSchema()

# Write to Bronze Delta table
bronze_query = (bronze_stream_df
    .writeStream
    .format("delta")  # Delta format
    .outputMode("append")  # Append mode
    .option("checkpointLocation", f"{checkpoint_dir}/bronze")
    .trigger(availableNow=True)  # availableNow=True
    .start(f"{working_dir}/bronze_taxi_trips")
)

# Wait for completion
bronze_query.awaitTermination()
bronze_query.stop()

print("✅ Bronze layer ingestion complete")

# COMMAND ----------

# Verify Bronze layer
bronze_df = spark.read.format("delta").load(f"{working_dir}/bronze_taxi_trips")
print(f"Bronze layer: {bronze_df.count():,} trips ingested")
display(bronze_df.limit(10))

# COMMAND ----------

# CHECK YOUR WORK
assert bronze_df.count() > 0, "Bronze table should contain data"
print("✅ Task 3.2 complete: Bronze layer created with Auto Loader")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.3: Streaming Aggregation - Silver Layer
# MAGIC
# MAGIC Create **hourly metrics by pickup zone** for real-time operations dashboard:
# MAGIC - Trips per hour per zone
# MAGIC - Total revenue per hour per zone
# MAGIC - Average trip distance per hour per zone
# MAGIC
# MAGIC **Key Challenge**: Use **windowed aggregation** with **watermark** to handle late-arriving data. Free Edition requires "append" output mode with watermarks for windowed aggregations.

# COMMAND ----------

# TODO: Create streaming aggregation with windowing and watermark
# Add watermark for late data handling, group by hourly windows

from pyspark.sql.functions import window, count, sum, avg, col, to_timestamp

# Read Bronze stream and cast timestamp columns
# Note: Auto Loader reads JSON timestamps as strings, so we need to cast them
silver_stream_df = (spark.readStream
    .format("delta")  # Delta format
    .load(f"{working_dir}/bronze_taxi_trips")
    .withColumn("tpep_pickup_datetime", to_timestamp(col("tpep_pickup_datetime")))
    .withColumn("tpep_dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime")))
    .withWatermark("tpep_pickup_datetime" , "10 minutes")  # Column and watermark interval
    .groupBy(
        window(col("tpep_pickup_datetime"), "1 hour"),  # Column and window duration
        col("pickup_zip")  # Additional grouping column
    )
    .agg(
        count("*").alias("trips_per_hour"),
        sum("fare_amount").alias("total_revenue"),  # Column to sum
        avg("trip_distance").alias("avg_trip_distance")  # Column to average
    )
)

# Write aggregated stream (use "append" mode - required for windowed aggregations in Free Edition)
silver_query = (silver_stream_df
    .writeStream
    .format("delta")  # Delta format
    .outputMode("append")  # Append mode for windowed aggregations
    .option("checkpointLocation", f"{checkpoint_dir}/silver")
    .trigger(availableNow=True)  # availableNow=True
    .start(f"{working_dir}/silver_hourly_metrics")
)

# Wait for completion
silver_query.awaitTermination()
silver_query.stop()

print("✅ Silver layer aggregation complete")

# COMMAND ----------

# Verify Silver layer
silver_df = spark.read.format("delta").load(f"{working_dir}/silver_hourly_metrics")
print(f"Silver layer: {silver_df.count():,} hourly zone metrics")
display(silver_df.orderBy("window", "pickup_zip"))

# COMMAND ----------

# CHECK YOUR WORK
assert silver_df.count() > 0, "Silver table should contain aggregated metrics"
assert "window" in silver_df.columns, "Should have window column"
assert "trips_per_hour" in silver_df.columns, "Should have aggregated metrics"
print("✅ Task 3.3 complete: Silver layer with windowed aggregations")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.4: Streaming MERGE - Gold Layer (Deduplication)
# MAGIC
# MAGIC Create the **Gold layer** by deduplicating streaming data using **MERGE** in a `foreachBatch` function. This ensures:
# MAGIC - No duplicate trips (same pickup time + locations)
# MAGIC - Idempotent pipeline (can rerun safely)
# MAGIC - Production-ready data for BI tools
# MAGIC
# MAGIC **Pattern**: `foreachBatch` allows you to run arbitrary code (including MERGE) on each streaming micro-batch.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Gold table (managed by Unity Catalog)
# MAGIC CREATE TABLE IF NOT EXISTS nyctaxi_catalog.analytics.taxi_trips_gold (
# MAGIC     tpep_pickup_datetime TIMESTAMP,
# MAGIC     tpep_dropoff_datetime TIMESTAMP,
# MAGIC     trip_distance DOUBLE,
# MAGIC     fare_amount DOUBLE,
# MAGIC     pickup_zip INT,
# MAGIC     dropoff_zip INT
# MAGIC ) USING DELTA;

# COMMAND ----------

# TODO
# Define foreachBatch function to MERGE streaming data

def upsert_to_gold(batch_df, batch_id):
    """
    Upsert function: MERGE streaming batch into Gold table
    - Update if trip already exists (same pickup time + locations)
    - Insert if trip is new
    """
    print(f"Processing batch {batch_id} with {batch_df.count()} records")

    # Register batch as temp view
    batch_df.createOrReplaceTempView("streaming_batch")

    # Execute MERGE
    spark.sql("""
        MERGE INTO nyctaxi_catalog.analytics.taxi_trips_gold AS target
        USING streaming_batch AS source
        ON target.tpep_pickup_datetime = source.tpep_pickup_datetime
           AND target.pickup_zip = source.pickup_zip
           AND target.dropoff_zip = source.dropoff_zip
        WHEN MATCHED THEN
            UPDATE SET
                tpep_dropoff_datetime = source.tpep_dropoff_datetime,
                trip_distance = source.trip_distance,
                fare_amount = source.fare_amount
        WHEN NOT MATCHED THEN
            INSERT (
                tpep_pickup_datetime, tpep_dropoff_datetime,
                trip_distance, fare_amount,
                pickup_zip, dropoff_zip
            )
            VALUES (
                source.tpep_pickup_datetime, source.tpep_dropoff_datetime,
                source.trip_distance, source.fare_amount,
                source.pickup_zip, source.dropoff_zip
            )
    """)

# Apply foreachBatch to streaming pipeline
# Note: Cast timestamps from string to timestamp type
gold_query = (spark.readStream
    .format("delta")  # Delta format
    .load(f"{working_dir}/bronze_taxi_trips")
    .withColumn("tpep_pickup_datetime", to_timestamp(col("tpep_pickup_datetime")))
    .withColumn("tpep_dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime")))
    .writeStream
    .foreachBatch(upsert_to_gold)  # Function name for batch processing
    .option("checkpointLocation", f"{checkpoint_dir}/gold")
    .trigger(availableNow=True)  # availableNow=True
    .start()
)

# Wait for completion
gold_query.awaitTermination()
gold_query.stop()

print("✅ Gold layer upsert complete")

# COMMAND ----------

# Verify Gold layer
gold_df = spark.table("nyctaxi_catalog.analytics.taxi_trips_gold")
print(f"Gold layer: {gold_df.count():,} unique trips")
display(gold_df.limit(10))

# COMMAND ----------

# CHECK YOUR WORK
assert gold_df.count() > 0, "Gold table should contain data"
# Count should be <= Bronze count (duplicates removed)
bronze_count = spark.read.format("delta").load(f"{working_dir}/bronze_taxi_trips").count()
assert gold_df.count() <= bronze_count, "Gold should have same or fewer records (duplicates removed)"
print("✅ Task 3.4 complete: Gold layer with streaming MERGE deduplication")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Conclusion
# MAGIC
# MAGIC Congratulations! You've built a complete **Delta Lake data pipeline** for NYC taxi analytics:
# MAGIC
# MAGIC ## What You've Accomplished:
# MAGIC
# MAGIC ✅ **Delta Lake Foundations** - Delta format, schema evolution, time travel, table history
# MAGIC ✅ **Data Management Operations** - CRUD operations, MERGE upserts, OPTIMIZE, Z-ordering, VACUUM
# MAGIC ✅ **Streaming Pipeline** - Auto Loader, Bronze → Silver → Gold architecture, windowed aggregations, streaming MERGE
# MAGIC
# MAGIC ## Key Takeaways:
# MAGIC
# MAGIC 1. **Delta Lake = ACID on Data Lakes** - Get database features (transactions, updates, deletes) on cheap object storage
# MAGIC 2. **Schema Evolution is Flexible** - Add columns with `mergeSchema`, change types with `overwriteSchema`
# MAGIC 3. **Time Travel is Powerful** - Audit changes, rollback mistakes, reproduce ML datasets
# MAGIC 4. **MERGE Simplifies Upserts** - Update + insert in one atomic operation
# MAGIC 5. **OPTIMIZE + Z-Order = Fast Queries** - Compact files and cluster related data together
# MAGIC 6. **Auto Loader = Scalable Ingestion** - Process millions of files with schema inference
# MAGIC 7. **Medallion Architecture = Best Practice** - Bronze (raw) → Silver (cleaned) → Gold (aggregated)
# MAGIC 8. **Free Edition is Production-Ready** - Same APIs as full Databricks, just with batch triggers
# MAGIC
# MAGIC ## Production Considerations:
# MAGIC
# MAGIC - **Retention**: Use `VACUUM RETAIN 168 HOURS` (7 days) in production to preserve time travel
# MAGIC - **Z-Order**: Choose columns used in WHERE clauses (not necessarily primary keys)
# MAGIC - **OPTIMIZE Frequency**: Run daily on hot tables, weekly on cold tables
# MAGIC - **Checkpoints**: Never delete checkpoint directories (breaks streaming pipelines)
# MAGIC - **Watermarks**: Balance between latency (small watermark) and completeness (large watermark)
# MAGIC
# MAGIC You've mastered Delta Lake management and streaming - essential skills for modern data engineering!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# Stop all active streaming queries
for query in spark.streams.active:
    print(f"Stopping query: {query.id}")
    query.stop()

# COMMAND ----------

# Optional: Clean up all lab data (uncomment to execute)
# dbutils.fs.rm(working_dir, recurse=True)
# spark.sql("DROP TABLE IF EXISTS nyctaxi_catalog.analytics.taxi_trips")
# spark.sql("DROP TABLE IF EXISTS nyctaxi_catalog.analytics.taxi_trips_gold")
# print("✅ Lab cleanup complete")
