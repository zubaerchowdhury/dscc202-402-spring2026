# Databricks notebook source
# MAGIC %md
# MAGIC # Bakehouse Real-Time Streaming
# MAGIC
# MAGIC ## Business Context
# MAGIC
# MAGIC Welcome to **The Bakehouse Real-Time Operations Center**! As Bakehouse franchises process thousands of transactions daily, headquarters needs live visibility into operations. The old batch reporting system has a 24-hour delay - by the time managers see yesterday's data, opportunities are missed and problems have escalated.
# MAGIC
# MAGIC As a **Streaming Engineer** at Bakehouse HQ, you've been tasked with building real-time dashboards that give instant insights into franchise performance, customer behavior, and revenue trends.
# MAGIC
# MAGIC ## Dataset Overview
# MAGIC
# MAGIC You'll work with streaming versions of the Bakehouse data from `samples.bakehouse`:
# MAGIC
# MAGIC | Table | Description | Row Count | Usage |
# MAGIC |-------|-------------|-----------|-------|
# MAGIC | `sales_transactions` | Individual purchases | 3,333 | Streaming source data |
# MAGIC | `sales_franchises` | Franchise locations | 48 | Stream enrichment |
# MAGIC | `sales_customers` | Customer information | 300 | Customer enrichment |
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC This comprehensive lab covers four core Structured Streaming topics compatible with Databricks Free Edition:
# MAGIC
# MAGIC 1. **Real-Time Sales Monitoring** - Build streaming pipelines with readStream/writeStream, triggers, and checkpoints
# MAGIC 2. **Hourly Performance Analytics** - Implement windowing, watermarks, and time-based aggregations
# MAGIC 3. **Traffic Source Analysis** - Analyze customer acquisition channels with stateful aggregations
# MAGIC 4. **Unified Dashboard Challenge** - Integrate multiple streaming queries into production dashboard
# MAGIC
# MAGIC **Note**: This lab uses `trigger(availableNow=True)` for batch-style processing compatible with Databricks Free Edition serverless compute. The Structured Streaming APIs you'll learn are identical to production continuous streaming - only the trigger mode differs!
# MAGIC
# MAGIC ## Streaming Journey
# MAGIC
# MAGIC **Act 1: Live Transactions** → Build basic streaming pipeline to monitor sales in real-time
# MAGIC **Act 2: Peak Hours Analysis** → Use windowing to identify busy periods for staffing optimization
# MAGIC **Act 3: Marketing Insights** → Track which customer channels drive the most revenue
# MAGIC **Act 4: Operations Dashboard** → Combine all metrics into comprehensive real-time dashboard
# MAGIC
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a catalog and schema for our streaming lab
# MAGIC CREATE CATALOG IF NOT EXISTS bakehouse_catalog;
# MAGIC
# MAGIC -- Create a schema (database) in the catalog
# MAGIC CREATE SCHEMA IF NOT EXISTS bakehouse_catalog.streaming_lab;
# MAGIC
# MAGIC -- Create a managed volume for file storage
# MAGIC CREATE VOLUME IF NOT EXISTS bakehouse_catalog.streaming_lab.workspace;

# COMMAND ----------

# Set up working directory using Unity Catalog volume
import os

# Use Unity Catalog managed volume for file storage
working_dir = "/Volumes/bakehouse_catalog/streaming_lab/workspace"
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
    dbutils.fs.rm(f"{working_dir}/streaming_source", recurse=True)
    dbutils.fs.rm(f"{working_dir}/checkpoints", recurse=True)
    dbutils.fs.rm(f"{working_dir}/test_stream", recurse=True)
    dbutils.fs.rm(f"{working_dir}/real_time_sales", recurse=True)
    dbutils.fs.rm(f"{working_dir}/hourly_verification", recurse=True)
    dbutils.fs.rm(f"{working_dir}/hourly_sales", recurse=True)
    dbutils.fs.rm(f"{working_dir}/high_value_filter", recurse=True)
    dbutils.fs.rm(f"{working_dir}/traffic_metrics", recurse=True)
    dbutils.fs.rm(f"{working_dir}/franchise_traffic_metrics", recurse=True)
    dbutils.fs.rm(f"{working_dir}/enriched_stream", recurse=True)
    dbutils.fs.rm(f"{working_dir}/dashboard_running_total", recurse=True)
    dbutils.fs.rm(f"{working_dir}/dashboard_hourly_trends", recurse=True)
    dbutils.fs.rm(f"{working_dir}/dashboard_traffic_performance", recurse=True)
    print(f"✅ Cleaned up working directory: {working_dir}")
except Exception as e:
    print(f"⚠️ Cleanup note: {e}")
    print("Continuing with lab...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification Utilities
# MAGIC
# MAGIC These utility functions help you verify your work throughout the lab.

# COMMAND ----------

from pyspark.sql.functions import col

def verify_schema(df, expected_columns):
    """Check if DataFrame has expected columns."""
    missing = set(expected_columns) - set(df.columns)
    if missing:
        print(f"❌ Missing columns: {missing}")
        return False
    print(f"✅ Schema correct: {len(expected_columns)} columns present")
    return True

def check_streaming_query(query_name):
    """Check if a streaming query is running."""
    active_queries = [q.name for q in spark.streams.active]
    if query_name in active_queries:
        print(f"✅ Streaming query '{query_name}' is active")
        return True
    else:
        print(f"❌ Streaming query '{query_name}' not found")
        return False

def inspect_stream_output(path, num_rows=5, description=""):
    """Read streaming output as batch DataFrame and display sample."""
    print(f"\n📊 {description}")
    df = spark.read.format("delta").load(path)
    display(df.limit(num_rows))
    print(f"Total rows in output: {df.count():,}")
    return df

print("✅ Verification utilities loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Important: Databricks Free Edition Compatibility
# MAGIC
# MAGIC This lab is designed for **Databricks Free Edition** with serverless compute, which has specific streaming limitations:
# MAGIC
# MAGIC - ❌ **Cannot use `display()` on streaming DataFrames** - Interactive streaming display not supported
# MAGIC - ❌ **No continuous streaming** - Cannot use default or time-based triggers
# MAGIC - ❌ **"update" output mode not supported** - Use "append" for windowed aggregations and "complete" for non-windowed aggregations
# MAGIC - ✅ **Use `trigger(availableNow=True)`** - Processes all available data in batch mode
# MAGIC - ✅ **Use watermarks with aggregations** - Required for append mode to work with windowed aggregations
# MAGIC - ✅ **Verify outputs** - Read streaming results as batch DataFrames for verification
# MAGIC
# MAGIC ## Pattern Used Throughout This Lab:
# MAGIC
# MAGIC 1. **Create streaming DataFrame** with `readStream`
# MAGIC 2. **Apply transformations** (filters, aggregations, joins)
# MAGIC 3. **Write with `writeStream`** using `trigger(availableNow=True)`
# MAGIC 4. **Verify by reading output** as batch DataFrame and using `display()`
# MAGIC
# MAGIC This approach simulates real-time streaming while working within Free Edition constraints. You're learning the exact same Structured Streaming APIs used in production - the only difference is the trigger mode!

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 1: Real-Time Sales Monitoring
# MAGIC
# MAGIC **Business Goal:** Build a live dashboard showing transactions as they happen across all franchises.
# MAGIC
# MAGIC In this section, you'll learn the fundamentals of Structured Streaming: creating streaming DataFrames, writing to Delta tables with checkpoints and triggers, and monitoring streaming queries.
# MAGIC
# MAGIC ## Key Concepts:
# MAGIC - **readStream**: Create a streaming DataFrame that reads from a source (Delta table, files, etc.)
# MAGIC - **writeStream**: Write streaming results to a sink (Delta table, files, etc.)
# MAGIC - **trigger(availableNow=True)**: Process all available data in batch mode (required for Free Edition)
# MAGIC - **Checkpoint**: Enable fault tolerance by saving query progress
# MAGIC - **Verification Pattern**: Write streams to Delta, then read as batch DataFrames to verify results

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.1: Prepare Streaming Source Data
# MAGIC
# MAGIC Load the Bakehouse transactions and add a synthetic `traffic_source` column to simulate customer acquisition channels. Write this enhanced dataset as a Delta table that we'll use as our streaming source.

# COMMAND ----------

# TODO
# Load transactions and add traffic_source column
# Values: "walk-in" (40%), "online-order" (25%), "delivery-app" (20%), "phone-order" (10%), "social-media" (5%)

from pyspark.sql.functions import col, rand, when, lit

# Load base transactions
transactions_df = spark.table("samples.bakehouse.sales_transactions")

# Add synthetic traffic source field
streaming_source_df = (transactions_df
    .withColumn("traffic_source",
        when(rand() < 0.40, lit("walk-in"))
        .when(rand() < 0.65, lit("online-order"))
        .when(rand() < 0.85, lit("delivery-app"))
        .when(rand() < 0.95, lit("phone-order"))
        .otherwise(lit("social-media"))
    )
)

# Write as Delta table (our streaming source)
(streaming_source_df
 .write
 .format("delta")
 .mode("overwrite")
 .save(f"{working_dir}/streaming_source")
)

print(f"✅ Created streaming source with {streaming_source_df.count():,} transactions")
display(streaming_source_df.limit(10))

# COMMAND ----------

# CHECK YOUR WORK
source_check_df = spark.read.format("delta").load(f"{working_dir}/streaming_source")
assert source_check_df.count() == 3333, "Should have 3,333 transactions"
assert "traffic_source" in source_check_df.columns, "Should have traffic_source column"
print("✅ Task 1.1 complete: Streaming source data prepared")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.2: Verify Streaming Source Data
# MAGIC
# MAGIC Create a streaming DataFrame from the Delta table source, write it to a test output location using `writeStream`, and then read it back as a batch DataFrame to verify the streaming pipeline works correctly.
# MAGIC
# MAGIC **Why This Matters**: In Databricks Free Edition, we cannot use `display()` directly on streaming DataFrames. Instead, we write streaming data to Delta tables and verify the output by reading it as a batch DataFrame. This pattern is essential for testing and validating streaming pipelines.

# COMMAND ----------

# TODO: Create streaming DataFrame and write to test location
# 1. Read stream: format="delta", load from f"{working_dir}/streaming_source"
# 2. Write stream: format="delta", outputMode="append", checkpoint, trigger, start path

streaming_df = (spark.readStream
    .format("delta")  # Delta format
    .load(f"{working_dir}/streaming_source")  # Path to streaming_source
)

# Write streaming data to test location
test_query = (streaming_df
    .writeStream
    .format("delta")  # Delta format
    .outputMode("append")  # "append" for non-aggregated data
    .option("checkpointLocation", f"{checkpoint_dir}/test_stream")  # Checkpoint path: f"{checkpoint_dir}/test_stream"
    .trigger(availableNow=True)  # availableNow=True for Free Edition
    .start(f"{working_dir}/test_stream")  # Output path: f"{working_dir}/test_stream"
)

# Wait for processing to complete
test_query.awaitTermination()
test_query.stop()

# Verify by reading output as batch DataFrame
test_output_df = spark.read.format("delta").load(f"{working_dir}/test_stream")
print(f"✅ Streaming verification: Processed {test_output_df.count():,} records")
display(test_output_df.limit(10))

# COMMAND ----------

# CHECK YOUR WORK
assert streaming_df.isStreaming, "DataFrame should be a streaming DataFrame"
assert test_output_df.count() > 0, "Should have written streaming data"
print("✅ Task 1.2 complete: Streaming source verified")
print("📝 Note: In Free Edition, we write streams and verify outputs by reading as batch DataFrames!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.3: Write Streaming Query with Checkpoint
# MAGIC
# MAGIC Write the streaming DataFrame to a Delta table using `writeStream`. Configure a checkpoint location to enable fault tolerance. If the query fails, it can resume from the checkpoint.
# MAGIC
# MAGIC **Important**:
# MAGIC - Checkpoints are essential for production streaming applications. They track processing progress so queries can recover from failures.
# MAGIC - **Output Modes**: For raw streaming data (no aggregations), use `"append"` mode. Only use `"update"` or `"complete"` for aggregated streams.

# COMMAND ----------

# TODO: Write streaming query with checkpoint
# Configure: format, outputMode="append", checkpoint location, trigger, output path
# Paths: checkpoint=f"{checkpoint_dir}/real_time_sales", output=f"{working_dir}/real_time_sales"

query = (streaming_df
    .writeStream
    .format("delta")  # Delta format
    .outputMode("append")  # "append" for non-aggregated data
    .option("checkpointLocation", f"{checkpoint_dir}/real_time_sales")  # f"{checkpoint_dir}/real_time_sales"
    .trigger(availableNow=True)  # availableNow=True for Free Edition
    .start(f"{working_dir}/real_time_sales")  # f"{working_dir}/real_time_sales"
)

# Wait for all data to be processed
query.awaitTermination()
query.stop()

print(f"✅ Streaming query completed")
print(f"Query ID: {query.id}")

# COMMAND ----------

# CHECK YOUR WORK
# Verify data was written
output_df = spark.read.format("delta").load(f"{working_dir}/real_time_sales")
assert output_df.count() > 0, "Should have written streaming data"

# Verify checkpoint exists
checkpoint_files = dbutils.fs.ls(f"{checkpoint_dir}/real_time_sales")
assert len(checkpoint_files) > 0, "Checkpoint directory should exist"

print(f"✅ Task 1.3 complete: Wrote {output_df.count():,} records with checkpointing")
print("📝 Checkpoint enables fault tolerance - query can resume from checkpoint if it fails")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.4: Monitor and Manage Streaming Queries
# MAGIC
# MAGIC Learn to check the status of active streaming queries, monitor their progress, and stop them properly. Proper query management is essential for production streaming applications.

# COMMAND ----------

# Start a streaming query to monitor
monitor_query = (spark.readStream
    .format("delta")
    .load(f"{working_dir}/streaming_source")
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{checkpoint_dir}/monitor_demo")
    .trigger(availableNow=True)
    .start(f"{working_dir}/monitor_demo")
)

# TODO: Access query properties
# Use monitor_query.id, monitor_query.isActive, monitor_query.status

print(f"Query ID: {monitor_query.id}")  # Get query ID
print(f"Is Active: {monitor_query.isActive}")  # Check if query is active
print(f"Status: {monitor_query.status}")  # Get query status

# List all active queries
print(f"\nAll active queries: {len(spark.streams.active)}")
for q in spark.streams.active:
    print(f"  - Query ID: {q.id}, Name: {q.name if q.name else 'Unnamed'}")

# Stop the query
monitor_query.stop()
print(f"\nQuery stopped. Is Active: {monitor_query.isActive}")

# COMMAND ----------

# CHECK YOUR WORK
assert not monitor_query.isActive, "Query should be stopped"
print("✅ Task 1.4 complete: Streaming query monitoring demonstrated")
print("📝 Always stop streaming queries when done to free up cluster resources")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 2: Hourly Performance Analytics
# MAGIC
# MAGIC **Business Goal:** Identify peak sales hours to optimize staffing levels and inventory management.
# MAGIC
# MAGIC In this section, you'll learn time-based windowing for aggregating streaming data by hour, watermarking to handle late-arriving data, and different output modes for writing aggregated streams.
# MAGIC
# MAGIC ## Key Concepts:
# MAGIC - **Windowing**: Group streaming data into time-based buckets (e.g., hourly, 15-minute intervals)
# MAGIC - **Watermark**: Define how long to wait for late data before finalizing a window (required for append mode with windowed aggregations)
# MAGIC - **trigger(availableNow=True)**: Process all available data in batch mode (simulates micro-batch processing)
# MAGIC - **Output Mode in Free Edition**:
# MAGIC   - **"update" mode not supported** in Databricks Free Edition serverless compute
# MAGIC   - Use **"append"** for windowed aggregations (requires watermarks, writes finalized windows only)
# MAGIC   - Use **"complete"** for non-windowed aggregations (writes entire result table each time)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.1: Add Watermark for Late Data Handling
# MAGIC
# MAGIC Configure a watermark to specify how long to wait for late-arriving transactions. A watermark of "10 minutes" means the system will wait up to 10 minutes for delayed transactions before considering a time window complete.

# COMMAND ----------

# TODO: Add watermark to handle late-arriving data
# Use withWatermark("dateTime", "10 minutes")
# This tells Spark to wait up to 10 minutes for late transactions

from pyspark.sql.functions import window, sum, count, avg

streaming_df = (spark.readStream
    .format("delta")
    .load(f"{working_dir}/streaming_source")
    .withWatermark("dateTime", "10 minutes")  # Column name and interval
)

print("✅ Streaming DataFrame with watermark created")
print(f"Watermark: Wait up to 10 minutes for late data")

# COMMAND ----------

# CHECK YOUR WORK
assert streaming_df.isStreaming, "Should be a streaming DataFrame"
print("✅ Task 2.1 complete: Watermark configured")
print("📝 Watermarks balance between waiting for late data and finalizing results promptly")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.2: Hourly Sales Aggregation with Windowing and Verification
# MAGIC
# MAGIC Use the `window()` function to group transactions into 1-hour buckets and calculate total sales and transaction count per hour per franchise. Write the aggregated stream to Delta and verify the results.
# MAGIC
# MAGIC **Why This Matters**: Windowing enables time-series analysis on streaming data, revealing patterns like peak hours, slow periods, and trends. We write the aggregated stream and verify by reading it back as a batch DataFrame.

# COMMAND ----------

# TODO: Aggregate streaming data by hour and write to Delta
# Use window() function to create 1-hour time buckets
# Group by window and franchiseID, then calculate aggregations

hourly_sales_df = (streaming_df
    .groupBy(
        window(col("dateTime"), "1 hour"),  # Column name and window duration
        col("franchiseID")  # Franchise column for grouping
    )
    .agg(
        sum("totalPrice").alias("total_sales"),  # Column to sum
        count("*").alias("transaction_count"),  # Column to count
        avg("totalPrice").alias("avg_transaction_value")  # Column to average
    )
)

# Write the aggregated stream to Delta
hourly_query = (hourly_sales_df
    .writeStream
    .format("delta")  # Delta format
    .outputMode("append")  # Output mode for windowed aggregations
    .option("checkpointLocation", f"{checkpoint_dir}/hourly_verification")  # Checkpoint path
    .trigger(availableNow=True)  # Trigger type
    .start(f"{working_dir}/hourly_verification")  # Output path
)

# Wait for processing to complete
hourly_query.awaitTermination()
hourly_query.stop()

# Verify by reading output as batch DataFrame
hourly_output_df = spark.read.format("delta").load(f"{working_dir}/hourly_verification")
print(f"✅ Hourly aggregation complete: {hourly_output_df.count()} time windows")
display(hourly_output_df.orderBy("window"))

# COMMAND ----------

# CHECK YOUR WORK
assert hourly_sales_df.isStreaming, "Should be a streaming DataFrame"
assert hourly_output_df.count() > 0, "Should have hourly aggregations"
print("✅ Task 2.2 complete: Hourly aggregation created and verified")
print("📝 The window column contains the start/end time of each hourly bucket")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.3: Write Aggregated Stream with Append Mode
# MAGIC
# MAGIC Write the hourly aggregations to a Delta table using "append" output mode. In Databricks Free Edition, only "append" mode is supported. For aggregations with watermarks, "append" mode writes only finalized windows (windows that won't receive more late data based on the watermark threshold).

# COMMAND ----------

# TODO: Write aggregated stream to Delta
# IMPORTANT: Use "append" mode (only mode supported in Free Edition)
# Configure checkpoint and output paths

query = (hourly_sales_df
    .writeStream
    .format("delta")  # Delta format
    .outputMode("append")  # Append mode
    .option("checkpointLocation", f"{checkpoint_dir}/hourly_sales")  # Checkpoint path
    .trigger(availableNow=True)  # availableNow=True
    .start(f"{working_dir}/hourly_sales")  # Output path for hourly_sales
)

# Process all available data
query.awaitTermination()
query.stop()

print(f"✅ Hourly sales stream written")

# COMMAND ----------

# CHECK YOUR WORK
hourly_results = spark.read.format("delta").load(f"{working_dir}/hourly_sales")
assert hourly_results.count() > 0, "Should have hourly aggregations"
assert "window" in hourly_results.columns, "Should have window column"
assert "total_sales" in hourly_results.columns, "Should have total_sales column"
print(f"✅ Task 2.3 complete: Wrote {hourly_results.count()} hourly aggregations")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.4: Query Historical Results to Find Peak Hours
# MAGIC
# MAGIC Read the aggregated hourly sales as a batch DataFrame and identify the busiest hours for staffing decisions.

# COMMAND ----------

# TODO: Read hourly sales results and find peak hours
# Read from Delta output and sort by total_sales descending

from pyspark.sql.functions import desc

peak_hours_df = (spark.read
    .format("delta")  # Delta format
    .load(f"{working_dir}/hourly_sales")  # Path to hourly_sales output
    .select(
        col("window.start").alias("hour_start"),
        col("window.end").alias("hour_end"),
        col("franchiseID"),
        col("total_sales"),
        col("transaction_count"),
        col("avg_transaction_value")
    )
    .orderBy(desc("total_sales"))  # Sort by total_sales descending
)

display(peak_hours_df)

# COMMAND ----------

# CHECK YOUR WORK
assert peak_hours_df.count() > 0, "Should have results"
print("✅ Task 2.4 complete: Peak hours identified")
print("📝 Management can use this to schedule staff during busy periods!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 3: Traffic Source Analysis
# MAGIC
# MAGIC **Business Goal:** Understand which customer acquisition channels (walk-in, online, delivery apps, etc.) drive the most revenue.
# MAGIC
# MAGIC In this section, you'll filter streaming data, perform multi-dimensional aggregations, and enrich streaming data with static reference tables.
# MAGIC
# MAGIC ## Key Concepts:
# MAGIC - **Stream Filtering**: Apply filters to streaming DataFrames
# MAGIC - **Multi-dimensional Aggregation**: Group by multiple columns
# MAGIC - **Stream-Static Joins**: Enrich streaming data with static lookup tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.1: Filter and Verify High-Value Transactions
# MAGIC
# MAGIC Focus on transactions above $50 to analyze premium customer behavior. Filter the streaming data, write to Delta, and verify the results.

# COMMAND ----------

# TODO: Read streaming data, filter for high-value transactions, write and verify
# Filter for transactions with totalPrice > 50

streaming_df = (spark.readStream
    .format("delta")
    .load(f"{working_dir}/streaming_source")
)

high_value_stream = streaming_df.filter(col("totalPrice") > 50)  # Filter condition

# Write filtered stream to Delta
filter_query = (high_value_stream
    .writeStream
    .format("delta")  # Delta format
    .outputMode("append")  # Append mode
    .option("checkpointLocation", f"{checkpoint_dir}/high_value_filter")  # Checkpoint path
    .trigger(availableNow=True)  # availableNow=True
    .start(f"{working_dir}/high_value_filter")  # Output path
)

# Wait for processing to complete
filter_query.awaitTermination()
filter_query.stop()

# Verify by reading output as batch DataFrame
high_value_output_df = spark.read.format("delta").load(f"{working_dir}/high_value_filter")
print(f"✅ Filtered {high_value_output_df.count():,} high-value transactions (> $50)")
display(high_value_output_df.orderBy(col("totalPrice").desc()).limit(10))

# COMMAND ----------

# CHECK YOUR WORK
assert high_value_stream.isStreaming, "Should be streaming"
assert high_value_output_df.count() > 0, "Should have high-value transactions"
print("✅ Task 3.1 complete: Filtered for high-value transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.2: Aggregate by Traffic Source
# MAGIC
# MAGIC Calculate total revenue, transaction count, and average order value for each customer acquisition channel.

# COMMAND ----------

# TODO: Aggregate streaming data by traffic_source
# Group by traffic source and calculate revenue metrics

traffic_metrics_df = (streaming_df
    .groupBy( # Column to group by
        "traffic_source",
        "franchiseID"
      )
    .agg(
        sum("totalPrice").alias("total_revenue"),  # Column to sum
        count("*").alias("transaction_count"),  # Column to count
        avg("totalPrice").alias("avg_order_value")  # Column to average
    )
)

# Write to Delta for analysis
# Note: Using append mode as it's the only mode supported in Free Edition
# For aggregations without event-time and watermark, this may not work as expected
# We need to add watermark to the streaming_df first
streaming_df_with_watermark = (spark.readStream
    .format("delta")
    .load(f"{working_dir}/streaming_source")
    .withWatermark("dateTime", "10 minutes")
)

traffic_metrics_df = (streaming_df_with_watermark
    .groupBy("traffic_source")
    .agg(
        sum("totalPrice").alias("total_revenue"),  # Column to sum
        count("*").alias("transaction_count"),  # Column to count
        avg("totalPrice").alias("avg_order_value")  # Column to average
    )
)

query = (traffic_metrics_df
    .writeStream
    .format("delta")
    .outputMode("complete")
    .option("checkpointLocation", f"{checkpoint_dir}/traffic_metrics")
    .trigger(availableNow=True)
    .start(f"{working_dir}/traffic_metrics")
)

query.awaitTermination()
query.stop()

# Read and display results
traffic_results = spark.read.format("delta").load(f"{working_dir}/traffic_metrics").orderBy(desc("total_revenue"))
display(traffic_results)

# COMMAND ----------

# CHECK YOUR WORK
assert traffic_results.count() > 0, "Should have traffic metrics"
assert "traffic_source" in traffic_results.columns, "Should have traffic_source"
print("✅ Task 3.2 complete: Traffic source metrics calculated")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.3: Multi-Dimensional Aggregation
# MAGIC
# MAGIC Aggregate by both traffic source AND franchise to identify which acquisition channels work best at which locations.

# COMMAND ----------

# TODO: Aggregate by traffic_source and franchiseID
# Multi-dimensional aggregation to analyze channels by location

franchise_traffic_metrics_df = (streaming_df
    .groupBy(
        "traffic_source",  # First grouping column
        "franchiseID" # Second grouping column
    )
    .agg(
        sum("totalPrice").alias("total_revenue"),  # Column to sum
        count("*").alias("transaction_count")  # Column to count
    )
)

# Write to Delta
query = (franchise_traffic_metrics_df
    .writeStream
    .format("delta")
    .outputMode("complete")
    .option("checkpointLocation", f"{checkpoint_dir}/franchise_traffic_metrics")
    .trigger(availableNow=True)
    .start(f"{working_dir}/franchise_traffic_metrics")
)

query.awaitTermination()
query.stop()

# Read results
detailed_results = spark.read.format("delta").load(f"{working_dir}/franchise_traffic_metrics").orderBy(desc("total_revenue"))
display(detailed_results)

# COMMAND ----------

# CHECK YOUR WORK
assert detailed_results.count() > 0, "Should have detailed metrics"
print("✅ Task 3.3 complete: Multi-dimensional aggregation complete")
print("📝 Marketing can now target specific channels for specific franchise locations!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.4: Enrich Streaming Data with Static Franchise Information
# MAGIC
# MAGIC Join the streaming transaction data with the static franchise table to add franchise names and locations to the stream.
# MAGIC
# MAGIC **Why This Matters**: Stream-static joins are common in production - you often need to enrich real-time events with reference data that changes slowly.

# COMMAND ----------

# TODO: Join streaming data with static franchises table
# Enrich stream with franchise name and city information

# Load static franchises data
franchises_df = spark.table("samples.bakehouse.sales_franchises")

# Join stream with static data
enriched_stream = (streaming_df
    .join(franchises_df, "franchiseID")  # DataFrame to join and join column
    .select(
        col("dateTime"),
        col("franchiseID"),
        franchises_df["name"].alias("franchise_name"),  # Franchise name column
        franchises_df["city"].alias("franchise_city"),  # Franchise city column
        col("traffic_source"),
        col("product"),
        col("totalPrice")
    )
)

# Write enriched stream
query = (enriched_stream
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{checkpoint_dir}/enriched_stream")
    .trigger(availableNow=True)
    .start(f"{working_dir}/enriched_stream")
)

query.awaitTermination()
query.stop()

# Display enriched results
enriched_results = spark.read.format("delta").load(f"{working_dir}/enriched_stream")
display(enriched_results)

# COMMAND ----------

# CHECK YOUR WORK
assert enriched_results.count() > 0, "Should have enriched data"
assert "franchise_name" in enriched_results.columns, "Should have franchise name"
assert "franchise_city" in enriched_results.columns, "Should have franchise city"
print("✅ Task 3.4 complete: Stream enriched with static data")
print("📝 Stream-static joins let you add context from slowly changing reference tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 4: Unified Streaming Dashboard Challenge
# MAGIC
# MAGIC **Business Goal:** Build a comprehensive real-time operations dashboard combining all metrics.
# MAGIC
# MAGIC ## Requirements:
# MAGIC 1. Read streaming transactions with watermark
# MAGIC 2. Calculate three metrics simultaneously:
# MAGIC    - Running total sales by franchise
# MAGIC    - Hourly sales trends
# MAGIC    - Traffic source performance
# MAGIC 3. Write each metric to separate Delta tables
# MAGIC 4. Monitor all streaming queries
# MAGIC
# MAGIC Apply everything you've learned: readStream, watermarks, windowing, aggregations, and query management!

# COMMAND ----------

# MAGIC %md
# MAGIC ### Challenge: Build Unified Streaming Dashboard
# MAGIC
# MAGIC Combine all streaming techniques into a production-ready real-time dashboard.

# COMMAND ----------

# TODO: Complete the unified dashboard
# Apply all streaming techniques: watermarks, windowing, aggregations

from pyspark.sql.functions import current_timestamp, sum, count, approx_count_distinct, avg

# Create streaming source with watermark
streaming_df = (spark.readStream
    .format("delta")  # Delta format
    .load(f"{working_dir}/streaming_source")  # Path to streaming source
    .withWatermark("dateTime", "10 minutes")  # Column name and watermark interval
)

# Metric 1: Running total sales by franchise
running_total_df = (streaming_df
    .groupBy("franchiseID")  # Column to group by
    .agg(
        sum("totalPrice").alias("total_sales"),  # Column to sum
        count("*").alias("total_transactions"),  # Column to count
        approx_count_distinct("customerID").alias("unique_customers")  # Column for distinct count
    )
)

# Metric 2: Hourly sales trends
hourly_trends_df = (streaming_df
    .groupBy(
        window(col("dateTime"), "1 hour"),  # Column and window duration
        col("franchiseID")  # Additional grouping column
    )
    .agg(
        sum("totalPrice").alias("hourly_sales"),  # Column to sum
        count("*").alias("hourly_transactions")  # Column to count
    )
)

# Metric 3: Traffic source performance
traffic_performance_df = (streaming_df
    .groupBy("traffic_source")  # Column to group by
    .agg(
        sum("totalPrice").alias("source_revenue"),  # Column to sum
        count("*").alias("source_transactions"),  # Column to count
        avg("totalPrice").alias("source_avg_value")  # Column to average
    )
)

# Write all streams with appropriate configurations
# IMPORTANT: Use "complete" for non-windowed aggregations, "append" for windowed aggregations
query1 = (running_total_df
    .writeStream
    .format("delta")  # Delta format
    .outputMode("complete")  # Complete mode for non-windowed
    .option("checkpointLocation", f"{checkpoint_dir}/dashboard_running_total")  # Checkpoint path
    .trigger(availableNow=True)  # availableNow=True
    .start(f"{working_dir}/dashboard_running_total")  # Output path
)

query2 = (hourly_trends_df
    .writeStream
    .format("delta")  # Delta format
    .outputMode("append")  # Append mode for windowed
    .option("checkpointLocation", f"{checkpoint_dir}/dashboard_hourly_trends")  # Checkpoint path
    .trigger(availableNow=True)  # availableNow=True
    .start(f"{working_dir}/dashboard_hourly_trends")  # Output path
)

query3 = (traffic_performance_df
    .writeStream
    .format("delta")  # Delta format
    .outputMode("complete")  # Complete mode for non-windowed
    .option("checkpointLocation", f"{checkpoint_dir}/dashboard_traffic_performance")  # Checkpoint path
    .trigger(availableNow=True)  # availableNow=True
    .start(f"{working_dir}/dashboard_traffic_performance")  # Output path
)

# Monitor all queries
print("📊 Unified Streaming Dashboard Launched!")
print(f"\nActive Queries: {len(spark.streams.active)}")
for query in [query1, query2, query3]:
    print(f"  - Query {query.id}: Active={query.isActive}")

# Wait for all queries to process data
query1.awaitTermination()
query2.awaitTermination()
query3.awaitTermination()

# Stop all queries
for query in [query1, query2, query3]:
    query.stop()

print("\n✅ All queries stopped successfully")

# COMMAND ----------

# CHECK YOUR WORK
# Verify all three metric tables were created and populated
running_total_results = spark.read.format("delta").load(f"{working_dir}/dashboard_running_total")
hourly_trends_results = spark.read.format("delta").load(f"{working_dir}/dashboard_hourly_trends")
traffic_performance_results = spark.read.format("delta").load(f"{working_dir}/dashboard_traffic_performance")

assert running_total_results.count() > 0, "Should have running total data"
assert hourly_trends_results.count() > 0, "Should have hourly trends data"
assert traffic_performance_results.count() > 0, "Should have traffic performance data"

print(f"✅ Challenge complete!")
print(f"\n📊 Dashboard Metrics Summary:")
print(f"  - Running Totals: {running_total_results.count()} franchises")
print(f"  - Hourly Trends: {hourly_trends_results.count()} time windows")
print(f"  - Traffic Performance: {traffic_performance_results.count()} channels")
print(f"\n🎉 Congratulations! You've built a production-ready streaming dashboard!")

# COMMAND ----------

# Display the dashboard results
print("📊 Running Total by Franchise:")
display(running_total_results.orderBy(desc("total_sales")))

# COMMAND ----------

print("📈 Hourly Sales Trends:")
hourly_display = hourly_trends_results.select(
    col("window.start").alias("hour_start"),
    col("franchiseID"),
    col("hourly_sales"),
    col("hourly_transactions")
).orderBy("hour_start", "franchiseID")
display(hourly_display)

# COMMAND ----------

print("🎯 Traffic Source Performance:")
display(traffic_performance_results.orderBy(desc("source_revenue")))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Cleanup
# MAGIC
# MAGIC Run the following cells to clean up your environment.

# COMMAND ----------

# Stop any remaining active queries
for query in spark.streams.active:
    print(f"Stopping query: {query.id}")
    query.stop()

print("✅ All streaming queries stopped")

# COMMAND ----------

# Clean up working directory
dbutils.fs.rm(f"{working_dir}/streaming_source", recurse=True)
dbutils.fs.rm(f"{working_dir}/checkpoints", recurse=True)
dbutils.fs.rm(f"{working_dir}/test_stream", recurse=True)
dbutils.fs.rm(f"{working_dir}/real_time_sales", recurse=True)
dbutils.fs.rm(f"{working_dir}/monitor_demo", recurse=True)
dbutils.fs.rm(f"{working_dir}/hourly_verification", recurse=True)
dbutils.fs.rm(f"{working_dir}/hourly_sales", recurse=True)
dbutils.fs.rm(f"{working_dir}/high_value_filter", recurse=True)
dbutils.fs.rm(f"{working_dir}/traffic_metrics", recurse=True)
dbutils.fs.rm(f"{working_dir}/franchise_traffic_metrics", recurse=True)
dbutils.fs.rm(f"{working_dir}/enriched_stream", recurse=True)
dbutils.fs.rm(f"{working_dir}/dashboard_running_total", recurse=True)
dbutils.fs.rm(f"{working_dir}/dashboard_hourly_trends", recurse=True)
dbutils.fs.rm(f"{working_dir}/dashboard_traffic_performance", recurse=True)
print(f"✅ Cleaned up working directory: {working_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Congratulations!
# MAGIC
# MAGIC You've completed the comprehensive Bakehouse Real-Time Streaming lab, covering:
# MAGIC
# MAGIC ✅ **Real-Time Sales Monitoring** - readStream, writeStream, triggers, checkpoints, verification patterns
# MAGIC ✅ **Hourly Performance Analytics** - Windowing, watermarks, time-based aggregations
# MAGIC ✅ **Traffic Source Analysis** - Filtering, multi-dimensional aggregations, stream-static joins
# MAGIC ✅ **Unified Dashboard** - Multiple concurrent streams, comprehensive monitoring
# MAGIC
# MAGIC ## Key Takeaways:
# MAGIC
# MAGIC 1. **Structured Streaming is DataFrame-based** - Use familiar DataFrame operations on streaming data
# MAGIC 2. **trigger(availableNow=True) enables batch-style processing** - Compatible with Databricks Free Edition serverless compute
# MAGIC 3. **Checkpoints enable fault tolerance** - Always configure checkpoints for production
# MAGIC 4. **Watermarks handle late data** - Balance between waiting for late data and timely results
# MAGIC 5. **Windowing enables time-series analysis** - Group streaming data into time buckets
# MAGIC 6. **Choose output mode based on aggregation type** - Use "append" for windowed aggregations, "complete" for non-windowed aggregations
# MAGIC 7. **Verify streaming outputs** - Write to Delta and read as batch DataFrames to check results
# MAGIC 8. **Monitor query health** - Check status, metrics, and stop queries properly
# MAGIC
# MAGIC These streaming skills are essential for building real-time data pipelines and operational dashboards! The Structured Streaming APIs you've learned work identically in production environments - just use continuous triggers instead of `availableNow` for true real-time processing!
