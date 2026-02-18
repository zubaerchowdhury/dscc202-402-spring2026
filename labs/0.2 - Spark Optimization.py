# Databricks notebook source
# MAGIC %md
# MAGIC # Bakehouse Performance Optimization
# MAGIC
# MAGIC ## Business Context
# MAGIC
# MAGIC Welcome back to **The Bakehouse**! As the franchise continues to grow, the Data Analytics team is facing new challenges. Dashboard queries that used to complete in seconds now take minutes. The monthly reporting pipeline is timing out. And the Marketing team has discovered duplicate customer records causing confusion in email campaigns.
# MAGIC
# MAGIC As a **Performance Engineer** at Bakehouse HQ, you've been tasked with diagnosing and resolving these performance bottlenecks while ensuring data quality across the organization.
# MAGIC
# MAGIC ## Dataset Overview
# MAGIC
# MAGIC You'll continue working with the Bakehouse data from `samples.bakehouse`:
# MAGIC
# MAGIC | Table | Description | Row Count | Usage |
# MAGIC |-------|-------------|-----------|-------|
# MAGIC | `sales_transactions` | Individual purchases | 3,333 | Query optimization, partitioning |
# MAGIC | `sales_customers` | Customer information | 300 | Base for duplicate generation |
# MAGIC | `sales_franchises` | Franchise locations | 48 | Join optimization |
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC This comprehensive lab covers four core Apache Spark performance topics:
# MAGIC
# MAGIC 1. **Query Optimization** - Analyze execution plans, leverage Catalyst optimizer, implement predicate pushdown
# MAGIC 2. **Partitioning** - Understand data distribution, use repartition vs coalesce, configure shuffle partitions
# MAGIC 3. **De-Duplication** - Remove duplicate records, implement case-insensitive matching, standardize data formats
# MAGIC 4. **Integration Challenge** - Combine optimization techniques into a production-ready pipeline
# MAGIC
# MAGIC ## Performance Journey
# MAGIC
# MAGIC **Act 1: Slow Dashboards** → Diagnose query performance issues and apply Catalyst optimizer techniques
# MAGIC **Act 2: Scale Challenges** → Implement partitioning strategies for efficient data distribution
# MAGIC **Act 3: Data Quality** → Clean duplicate customer records with advanced deduplication
# MAGIC **Act 4: Production Pipeline** → Integrate all optimizations into a comprehensive reporting system
# MAGIC
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a catalog for our performance lab
# MAGIC CREATE CATALOG IF NOT EXISTS bakehouse_catalog;
# MAGIC
# MAGIC -- Create a schema (database) in the catalog
# MAGIC CREATE SCHEMA IF NOT EXISTS bakehouse_catalog.performance_lab;
# MAGIC
# MAGIC -- Create a managed volume for file storage
# MAGIC CREATE VOLUME IF NOT EXISTS bakehouse_catalog.performance_lab.workspace;

# COMMAND ----------

# Set up working directory using Unity Catalog volume
import os

# Use Unity Catalog managed volume for file storage
working_dir = "/Volumes/bakehouse_catalog/performance_lab/workspace"

print(f"Working directory: {working_dir}")

# COMMAND ----------

# Clean up working directory to account for any failed previous runs.
dbutils.fs.rm(f"{working_dir}/transactions_partitioned", recurse=True)
dbutils.fs.rm(f"{working_dir}/repartitioned_demo", recurse=True)
dbutils.fs.rm(f"{working_dir}/coalesced_demo", recurse=True)
dbutils.fs.rm(f"{working_dir}/customers_with_duplicates", recurse=True)
dbutils.fs.rm(f"{working_dir}/customers_deduplicated", recurse=True)
dbutils.fs.rm(f"{working_dir}/franchise_performance_report", recurse=True)
print(f"✅ Cleaned up working directory: {working_dir}")

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

def check_partition_count(path, expected_count=None):
    """Check number of partitions in Delta table."""
    files = dbutils.fs.ls(path)
    partition_count = len([f for f in files if f.name.startswith('part-')])
    if expected_count and partition_count != expected_count:
        print(f"⚠️ Found {partition_count} partitions, expected {expected_count}")
        return False
    print(f"✅ Partition count: {partition_count}")
    return True

def inspect_sample(df, num_rows=5, description=""):
    """Display sample rows for manual inspection."""
    print(f"\n📊 Sample Data: {description}")
    display(df.limit(num_rows))
    print(f"Total rows: {df.count():,}")

print("✅ Verification utilities loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 1: Query Optimization Fundamentals
# MAGIC
# MAGIC **Business Goal:** Dashboard queries are taking 2+ minutes to complete. Management wants sub-second response times.
# MAGIC
# MAGIC In this section, you'll learn to diagnose slow queries using execution plans, understand how the Catalyst optimizer works, and implement optimization techniques like predicate pushdown and filter ordering.
# MAGIC
# MAGIC ## Key Concepts:
# MAGIC - **Catalyst Optimizer**: Spark's query optimizer that automatically improves query execution
# MAGIC - **Logical Plan**: High-level description of what the query does
# MAGIC - **Physical Plan**: Low-level description of how Spark will execute the query
# MAGIC - **Predicate Pushdown**: Moving filters closer to the data source to reduce data transfer

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.1: Analyze Slow Query with Explain Plans
# MAGIC
# MAGIC Load the transactions data and apply multiple filters. Use the `explain()` method to view how Catalyst optimizes your query by consolidating redundant filters.

# COMMAND ----------

# TODO: Load transactions table
# Use spark.table() to load samples.bakehouse.sales_transactions

from pyspark.sql.functions import col

transactions_df = spark.table("samples.bakehouse.sales_transactions")

# Apply multiple filters (some redundant)
slow_query_df = (transactions_df
    .filter(col("totalPrice") > 20)
    .filter(col("totalPrice") > 10)  # Redundant - already filtered > 20
    .filter(col("product") != "cookies")
    .filter(col("product") != "bread")
)

# Display the logical and physical plans
slow_query_df.explain(True)

# COMMAND ----------

# CHECK YOUR WORK
assert 'transactions_df' in dir(), "transactions_df should be defined"
assert transactions_df.count() == 3333, "Should load all 3,333 transactions"
print("✅ Task 1.1 complete: Execution plan displayed")
print("📝 Note: Look at the 'Optimized Logical Plan' - Catalyst consolidated the filters!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.2: Demonstrate Predicate Pushdown with Partitioned Delta
# MAGIC
# MAGIC Write the transactions data as a partitioned Delta table (partitioned by `franchiseID`). Then read it back with a filter and observe how Spark prunes partitions in the execution plan.
# MAGIC
# MAGIC **Why This Matters**: Predicate pushdown reduces data transfer by filtering at the storage layer instead of after loading all data.

# COMMAND ----------

# TODO: Write partitioned Delta table and read with filter
# 1. Partition by "franchiseID" column
# 2. Filter for franchiseID == 3000033 when reading back

(transactions_df
 .write
 .partitionBy("franchiseID")  # Which column to partition by?
 .format("delta")
 .mode("overwrite")
 .save(f"{working_dir}/transactions_partitioned")
)

# Read with filter - Spark will only read relevant partitions
filtered_df = spark.read.format("delta").load(
    f"{working_dir}/transactions_partitioned"
).filter(col("franchiseID") == 3000033)  # Filter condition: col("franchiseID") == 3000033

# Display the execution plan
filtered_df.explain(True)

display(filtered_df)

# COMMAND ----------

# CHECK YOUR WORK
assert filtered_df.count() > 0, "Should have results for franchiseID = 3000033"
print(f"✅ Task 1.2 complete: Filtered to {filtered_df.count()} transactions with partition pruning")
print("📝 Look at the explain plan output above - you should see PartitionFilters showing Spark pruned partitions!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.3: Optimize Join Query with Filter Pushdown
# MAGIC
# MAGIC Compare two approaches:
# MAGIC 1. **Inefficient**: Join first, then filter
# MAGIC 2. **Efficient**: Filter before join
# MAGIC
# MAGIC Observe the dramatic difference in rows processed.

# COMMAND ----------

# Load franchises data
franchises_df = spark.table("samples.bakehouse.sales_franchises")

# INEFFICIENT APPROACH: Join then filter
slow_join_df = (transactions_df
    .join(franchises_df, "franchiseID")
    .filter(col("country") == "US")
)

print(f"Inefficient approach processes {transactions_df.count()} transactions")
print(f"Filtered result: {slow_join_df.count()} rows")

# TODO: Optimize join by filtering before joining
# 1. Filter franchises_df where country == "US"
# 2. Join transactions_df with filtered franchises on "franchiseID"

fast_franchises_df = franchises_df.filter(col("country") == "US")  # Filter condition for US
fast_join_df = transactions_df.join(fast_franchises_df, "franchiseID")  # Join with filtered DataFrame, join column

print(f"\nEfficient approach joins with only {fast_franchises_df.count()} franchises")
print(f"Same filtered result: {fast_join_df.count()} rows")

# COMMAND ----------

# CHECK YOUR WORK
assert slow_join_df.count() == fast_join_df.count(), "Both approaches should return same results"
assert fast_franchises_df.count() < franchises_df.count(), "Should filter franchises before join"
print("✅ Task 1.3 complete: Join optimization demonstrated")
print(f"📊 Efficiency gain: Reduced franchise table from {franchises_df.count()} to {fast_franchises_df.count()} rows before join")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 2: Partitioning for Performance
# MAGIC
# MAGIC **Business Goal:** As data grows, queries are slowing down. We need to distribute workload efficiently across our cluster.
# MAGIC
# MAGIC In this section, you'll learn to inspect partition counts, use repartition vs coalesce, configure shuffle partitions, and leverage Adaptive Query Execution (AQE).
# MAGIC
# MAGIC ## Key Concepts:
# MAGIC - **Partition**: A chunk of data distributed across the cluster
# MAGIC - **repartition()**: Full shuffle to create evenly balanced partitions
# MAGIC - **coalesce()**: Narrow transformation to reduce partitions without full shuffle
# MAGIC - **Shuffle Partitions**: Number of partitions created during wide transformations (joins, aggregations)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.1: Repartition for Balanced Distribution
# MAGIC
# MAGIC Use `repartition()` to redistribute data evenly across 8 partitions. This performs a full shuffle. We'll verify by writing the data and counting output files.

# COMMAND ----------

# TODO: Repartition to 8 partitions
# Use .repartition(8) method on transactions_df

repartitioned_df = transactions_df.repartition(8)

# Write to verify partition count through file output
(repartitioned_df
 .write
 .mode("overwrite")
 .format("delta")
 .save(f"{working_dir}/repartitioned_demo")
)

# Count the data files (each partition creates one file)
output_files = dbutils.fs.ls(f"{working_dir}/repartitioned_demo")
data_files = [f for f in output_files if f.name.endswith('.parquet')]
print(f"Output files created: {len(data_files)}")
print(f"→ Each partition writes one file, so we have {len(data_files)} partitions")

# COMMAND ----------

# CHECK YOUR WORK
assert len(data_files) == 8, f"Should have exactly 8 files (partitions), got {len(data_files)}"
reloaded_count = spark.read.format("delta").load(f"{working_dir}/repartitioned_demo").count()
assert reloaded_count == transactions_df.count(), "Row count should remain the same"
print("✅ Task 2.1 complete: Repartitioned to 8 partitions")
print("📝 Note: repartition() triggers a full shuffle but ensures even distribution")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.2: Coalesce for Narrow Transformation
# MAGIC
# MAGIC Use `coalesce()` to reduce partitions without a full shuffle. Observe the key difference: coalesce is efficient but cannot increase partition count.

# COMMAND ----------

# TODO: Coalesce to reduce partitions
# Use .coalesce(2) method to reduce to 2 partitions

# Load the repartitioned data from Task 2.1
base_df = spark.read.format("delta").load(f"{working_dir}/repartitioned_demo")

coalesced_df = base_df.coalesce(2)

# Write and verify
(coalesced_df
 .write
 .mode("overwrite")
 .format("delta")
 .save(f"{working_dir}/coalesced_demo")
)

output_files = dbutils.fs.ls(f"{working_dir}/coalesced_demo")
data_files = [f for f in output_files if f.name.endswith('.parquet')]
print(f"After coalesce(2): {len(data_files)} files (partitions)")
print(f"→ Reduced from 8 to {len(data_files)} partitions without full shuffle!")

# COMMAND ----------

# CHECK YOUR WORK
assert len(data_files) <= 2, f"Should have at most 2 files (partitions), got {len(data_files)}"
assert len(data_files) < 8, f"Should have fewer files than before repartitioning (8), got {len(data_files)}"
assert coalesced_df.count() == base_df.count(), "Row count should remain the same"
print("✅ Task 2.2 complete: Coalesce behavior understood")
print("📝 Use coalesce() to reduce partitions cheaply, repartition() to increase or rebalance")
print("💡 Note: With small datasets, Delta may optimize to fewer files than the coalesce number")

# COMMAND ----------

# MAGIC %md
# MAGIC **Understanding Shuffle Partitions & Adaptive Query Execution:**
# MAGIC
# MAGIC ### Shuffle Partitions
# MAGIC When Spark performs wide transformations (joins, aggregations, sorts), it shuffles data across partitions. The number of partitions created during shuffles is controlled by `spark.sql.shuffle.partitions`.
# MAGIC
# MAGIC **Key considerations:**
# MAGIC - Default is often 200 partitions (too many for small datasets)
# MAGIC - Best practice: 2-4x your core count for small data
# MAGIC - Too many partitions = overhead, too few = underutilization
# MAGIC
# MAGIC ### Adaptive Query Execution (AQE)
# MAGIC Databricks serverless compute has AQE automatically enabled. AQE dynamically optimizes queries at runtime by:
# MAGIC - **Coalescing shuffle partitions** based on actual data size
# MAGIC - **Optimizing join strategies** (broadcast vs shuffle)
# MAGIC - **Handling data skew** automatically
# MAGIC
# MAGIC This means Spark automatically adjusts partition counts for optimal performance, even if you start with a high number!

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 3: De-Duplicating Customer Data
# MAGIC
# MAGIC **Business Goal:** Marketing reports duplicate customers receiving multiple emails. We need to clean the customer database.
# MAGIC
# MAGIC In this section, you'll generate synthetic duplicates, attempt simple deduplication, implement case-insensitive matching, standardize data formats, and write optimized output.
# MAGIC
# MAGIC ## Key Concepts:
# MAGIC - **dropDuplicates()**: Remove duplicate rows based on column values
# MAGIC - **Case-Insensitive Matching**: "John" = "JOHN" = "john"
# MAGIC - **Data Standardization**: "123-45-6789" = "123456789"
# MAGIC - **Single File Output**: repartition(1) for consolidated results

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.1: Generate Duplicate Customer Dataset
# MAGIC
# MAGIC Create synthetic duplicates from the base customer data by introducing case variations, spacing differences, and format inconsistencies.

# COMMAND ----------

# TODO
# Generate ~103K customer records with duplicates
# Use explode() to repeat customers multiple times, then add variations

from pyspark.sql.functions import lit, concat, when, upper, lower, expr, explode

# Load base customers
base_customers_df = spark.table("samples.bakehouse.sales_customers")

print(f"Base customers: {base_customers_df.count()}")

# Create duplicates by repeating each customer ~350 times with variations
# Generate an array of 350 elements using sequence(), then explode to create 350 rows per customer
duplicates_df = (base_customers_df
    .withColumn("duplicate_copies", explode(expr("sequence(0, 349)")))

    # Use duplicate_copies as the variation seed
    .withColumn("duplicate_id", col("duplicate_copies"))

    # Add case variations to first_name
    .withColumn("first_name",
        when(col("duplicate_id") % 3 == 0, upper(col("first_name")))
        .otherwise(col("first_name"))
    )

    # TODO: Add case variations to last_name
    # Use when(col("duplicate_id") % 2 == 0, ...) to uppercase some last names
    .withColumn("last_name",
        when(col("duplicate_id") % 2 == 0, upper(col("last_name")))
        .otherwise(col("last_name"))
    )

    # TODO: Add variations to email (some uppercase domain)
    # Use when(col("duplicate_id") % 4 == 0, ...) to uppercase some emails
    .withColumn("email_address",
        when(col("duplicate_id") % 4 == 0, upper(col("email_address")))
        .otherwise(col("email_address"))
    )

    # Drop the temporary column
    .drop("duplicate_copies")
)

# Write to volume
(duplicates_df
 .write
 .mode("overwrite")
 .format("delta")
 .save(f"{working_dir}/customers_with_duplicates")
)

# Check the count
dup_count = spark.read.format("delta").load(f"{working_dir}/customers_with_duplicates").count()
print(f"Generated {dup_count:,} customer records (including duplicates)")

# COMMAND ----------

# CHECK YOUR WORK
assert dup_count > 50000, f"Should generate significant duplicates, got {dup_count}"
print(f"✅ Task 3.1 complete: Generated {dup_count:,} records with duplicates")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.2: Simple Deduplication Attempt
# MAGIC
# MAGIC Try using `dropDuplicates()` on the raw data. Discover that it misses case-sensitive duplicates.

# COMMAND ----------

# Read duplicates
dups_df = spark.read.format("delta").load(f"{working_dir}/customers_with_duplicates")

# TODO: Apply simple deduplication
# Use dropDuplicates() on key columns: customerID, first_name, last_name, email_address
# Pass columns as a list

simple_dedup_df = dups_df.dropDuplicates(["customerID", "first_name", "last_name", "email_address"])  # List of column names

print(f"Original: {dups_df.count():,}")
print(f"After simple dedup: {simple_dedup_df.count():,}")
print(f"Removed: {dups_df.count() - simple_dedup_df.count():,} records")
print("\n⚠️ Still has duplicates due to case sensitivity!")
print("   'John' != 'JOHN' in simple dropDuplicates")

# COMMAND ----------

# CHECK YOUR WORK
assert simple_dedup_df.count() < dups_df.count(), "Should remove some duplicates"
assert simple_dedup_df.count() > 300, "Should still have many duplicates due to case sensitivity"
print("✅ Task 3.2 complete: Simple deduplication attempted")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.3: Case-Insensitive Deduplication
# MAGIC
# MAGIC Create lowercase versions of text columns and use them for deduplication, then drop the temporary columns.

# COMMAND ----------

# TODO: Case-insensitive deduplication
# 1. Create lowercase columns using lower(col(...))
# 2. Drop duplicates based on lowercase columns
# 3. Drop temporary lowercase columns

from pyspark.sql.functions import lower

normalized_df = (dups_df
    .withColumn("lcFirstName", lower(col("first_name")))  # Lowercase which column?
    .withColumn("lcLastName", lower(col("last_name")))  # lower(col("last_name"))
    .withColumn("lcEmail", lower(col("email_address")))  # lower(col("email_address"))
)

# Drop duplicates based on normalized columns
deduped_df = normalized_df.dropDuplicates(["lcFirstName", "lcLastName", "lcEmail"])  # List: ["lcFirstName", "lcLastName", "lcEmail"]

# Clean up temporary columns
final_df = deduped_df.drop("lcFirstName", "lcLastName", "lcEmail")  # Drop lcFirstName, lcLastName, lcEmail

print(f"After case-insensitive dedup: {final_df.count():,}")
print(f"Additional duplicates removed: {simple_dedup_df.count() - final_df.count():,}")

# COMMAND ----------

# CHECK YOUR WORK
assert final_df.count() < simple_dedup_df.count(), "Should remove more duplicates than simple method"
assert "lcFirstName" not in final_df.columns, "Should drop temporary columns"
expected_count = 300  # Should be close to base customer count
tolerance = 50
assert abs(final_df.count() - expected_count) < tolerance, f"Should have ~{expected_count} unique customers"
print("✅ Task 3.3 complete: Case-insensitive deduplication successful")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.4: Data Standardization for Better Matching
# MAGIC
# MAGIC The Bakehouse customers don't have SSN fields in the actual data, so we'll demonstrate the standardization concept using the postal_zip_code field instead, removing any formatting inconsistencies.

# COMMAND ----------

# TODO
# Demonstrate standardization concept by creating normalized columns
# In a real scenario, you'd standardize phone numbers, SSNs, etc.

from pyspark.sql.functions import translate, regexp_replace, trim

# For demonstration: normalize postal codes and phone numbers
standardized_df = (dups_df
    .withColumn("lcFirstName", lower(col("first_name")))
    .withColumn("lcLastName", lower(col("last_name")))
    .withColumn("lcEmail", lower(col("email_address")))
    .withColumn("cleanZip", regexp_replace(col("postal_zip_code"), "[^0-9]", "")) # Remove non-numeric chars
    # Normalize phone numbers (remove dashes, spaces, parentheses)
    .withColumn("cleanPhone",
        regexp_replace(regexp_replace(col("phone_number"), "[^0-9]", ""), " ", ""))
)

# Dedup including standardized fields
final_standardized_df = (standardized_df
    .dropDuplicates([
        "lcFirstName", "lcLastName", "lcEmail",
        "cleanPhone", "postal_zip_code", "gender"
    ])
    .drop("lcFirstName", "lcLastName", "lcEmail", "cleanPhone")
)

print(f"After standardization: {final_standardized_df.count():,}")
print(f"Further improved matching: {final_df.count() - final_standardized_df.count():,} more duplicates removed")

# COMMAND ----------

# CHECK YOUR WORK
assert final_standardized_df.count() <= final_df.count(), "Should not increase count"
print("✅ Task 3.4 complete: Data standardization applied")
print("📝 Standardization catches duplicates with formatting differences")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.5: Write Single Partition Output
# MAGIC
# MAGIC Write the deduplicated data as a single file for downstream systems.

# COMMAND ----------

# TODO: Write to single partition
# Use .repartition(1) to create a single output file

(final_standardized_df
 .repartition(1)  # How many partitions for single file?
 .write
 .mode("overwrite")
 .format("delta")
 .save(f"{working_dir}/customers_deduplicated")
)

# Verify single file
output_files = dbutils.fs.ls(f"{working_dir}/customers_deduplicated")
data_files = [f for f in output_files if f.name.endswith('.parquet')]
print(f"Output files: {len(data_files)} data file(s)")

# COMMAND ----------

# CHECK YOUR WORK
deduped_count = spark.read.format("delta").load(f"{working_dir}/customers_deduplicated").count()
assert deduped_count == final_standardized_df.count(), "Counts should match"
print(f"✅ Task 3.5 complete: Deduplicated {deduped_count:,} clean customer records")
print(f"📊 Summary: {dups_df.count():,} → {deduped_count:,} customers ({((1 - deduped_count/dups_df.count()) * 100):.1f}% duplicates removed)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 4: Comprehensive Performance Challenge
# MAGIC
# MAGIC **Business Goal:** Build an optimized monthly reporting pipeline that combines all performance techniques.
# MAGIC
# MAGIC ## Requirements:
# MAGIC 1. Load deduplicated customers
# MAGIC 2. Join with transactions (optimize join order)
# MAGIC 3. Join with franchises (filter before join)
# MAGIC 4. Aggregate metrics by franchise
# MAGIC 5. Write results with appropriate partitioning
# MAGIC
# MAGIC Apply everything you've learned: predicate pushdown, filter ordering, and efficient joins!

# COMMAND ----------

# MAGIC %md
# MAGIC ### Challenge: Build Optimized Reporting Pipeline
# MAGIC
# MAGIC Combine query optimization, partitioning, and clean data into a production-ready pipeline.

# COMMAND ----------

# TODO: Build optimized reporting pipeline
# Apply all performance techniques learned:
# - Load deduplicated data
# - Filter before joining (predicate pushdown)
# - Proper join order

from pyspark.sql.functions import sum, count, countDistinct, desc

# Step 1: Load deduplicated customers
clean_customers_df = spark.read.format("delta").load(f"{working_dir}/customers_deduplicated") # Load from f"{working_dir}/customers_deduplicated"

# Step 2: Load and filter franchises to US only (predicate pushdown!)
usa_franchises_df = spark.table("samples.bakehouse.sales_franchises").filter(col("country") == "US") # Load franchises and filter country == "USA"

# Step 3: Join transactions with clean customers, then with USA franchises
# Use "customerID" for customer join, "franchiseID" for franchise join
enriched_transactions_df = (transactions_df
    .join(clean_customers_df, "customerID")  # Join with clean_customers_df on "customerID"
    .join(usa_franchises_df, "franchiseID")  # Join with usa_franchises_df on "franchiseID"
)

# Step 4: Calculate franchise performance metrics
# Note: After joining multiple tables, some columns (name, city, country) exist in both
# customers and franchises DataFrames. You must disambiguate these columns using
# the DataFrame reference syntax: usa_franchises_df["column_name"]
franchise_report_df = (enriched_transactions_df
    .groupBy(
          "franchiseID",
          usa_franchises_df["name"], # disambiguate franchise name
          usa_franchises_df["city"], # disambiguate franchise city
          usa_franchises_df["country"] # disambiguate franchise country
    )
    .agg(
          sum("totalPrice").alias("total_revenue"),
          count("transactionID").alias("transaction_count"),
          countDistinct("customerID").alias("unique_customers")
    )
    .orderBy(desc("total_revenue"))  # desc("total_revenue")
)

display(franchise_report_df)

# Step 5: Write optimized output
(franchise_report_df
 .repartition(1)  # Choose appropriate partition count (e.g., 1-4)
 .write
 .mode("overwrite")
 .format("delta")
 .save(f"{working_dir}/franchise_performance_report")
)

# COMMAND ----------

# CHECK YOUR WORK
report_df = spark.read.format("delta").load(f"{working_dir}/franchise_performance_report")
assert report_df.count() > 0, "Should have franchise performance data"
assert "franchiseID" in report_df.columns, "Should include franchiseID"
print(f"✅ Challenge complete: Generated report for {report_df.count()} US franchises")
print("🎉 Congratulations! You've mastered Spark performance optimization!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Cleanup
# MAGIC
# MAGIC Run the following cell to clean up your environment.

# COMMAND ----------

# Clean up working directory
dbutils.fs.rm(f"{working_dir}/transactions_partitioned", recurse=True)
dbutils.fs.rm(f"{working_dir}/repartitioned_demo", recurse=True)
dbutils.fs.rm(f"{working_dir}/coalesced_demo", recurse=True)
dbutils.fs.rm(f"{working_dir}/customers_with_duplicates", recurse=True)
dbutils.fs.rm(f"{working_dir}/customers_deduplicated", recurse=True)
dbutils.fs.rm(f"{working_dir}/franchise_performance_report", recurse=True)
print(f"✅ Cleaned up working directory: {working_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Congratulations!
# MAGIC
# MAGIC You've completed the comprehensive Bakehouse Performance Optimization lab, covering:
# MAGIC
# MAGIC ✅ **Query Optimization** - Catalyst optimizer, explain plans, predicate pushdown, join optimization
# MAGIC ✅ **Partitioning** - repartition vs coalesce, shuffle configuration, AQE
# MAGIC ✅ **De-Duplication** - Case-insensitive matching, data standardization, efficient output
# MAGIC ✅ **Integration** - Combined techniques in production pipeline
# MAGIC
# MAGIC ## Key Takeaways:
# MAGIC
# MAGIC 1. **Always check explain plans** - Understand what Spark is actually doing
# MAGIC 2. **Filter early, filter often** - Push predicates close to data sources
# MAGIC 3. **Partition wisely** - Balance parallelism with overhead
# MAGIC 4. **Match data size to method** - Small data different from big data
# MAGIC 5. **Standardize before deduplication** - Clean data improves matching
# MAGIC
# MAGIC These performance optimization skills are essential for building scalable, production-ready data pipelines!
