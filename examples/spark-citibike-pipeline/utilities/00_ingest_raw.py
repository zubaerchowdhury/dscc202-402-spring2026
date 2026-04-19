# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# notebooks/00_ingest_raw.py
#
# PURPOSE : Bootstraps Unity Catalog objects (catalog / schema / volume) and
#           downloads Citi Bike trip CSVs from the public S3 bucket into the
#           UC Volume.
#
# DATE PARAMS (set via widgets or job base_parameters):
#   start_date  YYYY-MM-DD  first month to pull (inclusive)
#   end_date    YYYY-MM-DD  last  month to pull (inclusive)
#   If both are empty the notebook defaults to a daily-run mode: it attempts
#   to pull the two most recently published months.  Idempotency means already-
#   downloaded months are skipped, so this is safe to run every day.
#
# RUN ON  : Serverless compute (NOT inside the DLT pipeline).
# SCHEDULE: Daily via the citibike_daily_job Databricks Workflow.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 00 – Bootstrap Unity Catalog and ingest raw Citi Bike trip data

# COMMAND ----------

# MAGIC %pip install python-dateutil
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import io
import os
import zipfile
from datetime import datetime, timedelta

import requests
from dateutil.relativedelta import relativedelta

# ── Widgets ───────────────────────────────────────────────────────────────────
# When triggered by the daily job both values are empty strings → daily-default
# mode.  When run manually, fill in both to backfill a specific date range.
dbutils.widgets.text("start_date", "", "Start date (YYYY-MM-DD, optional)")
dbutils.widgets.text("end_date",   "", "End date   (YYYY-MM-DD, optional)")

start_date_raw = dbutils.widgets.get("start_date").strip()
end_date_raw   = dbutils.widgets.get("end_date").strip()

# ── Config ────────────────────────────────────────────────────────────────────
UC_CATALOG      = "citibike_catalog"
UC_SCHEMA       = "citibike"
UC_VOLUME       = "raw_data"
RAW_VOLUME_PATH = f"/Volumes/{UC_CATALOG}/{UC_SCHEMA}/{UC_VOLUME}"
BASE_URL        = "https://s3.amazonaws.com/tripdata"

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1 – Create catalog, schema, and volume (idempotent)

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {UC_CATALOG}")
print(f"✓ Catalog  : {UC_CATALOG}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}")
print(f"✓ Schema   : {UC_CATALOG}.{UC_SCHEMA}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}.{UC_VOLUME}")
print(f"✓ Volume   : {UC_CATALOG}.{UC_SCHEMA}.{UC_VOLUME}")
print(f"  FUSE path: {RAW_VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2 – Resolve target months

# COMMAND ----------

def months_in_range(start: str, end: str) -> list[str]:
    """
    Return every YYYYMM between two YYYY-MM-DD date strings, inclusive.
    Example: ("2024-09-15", "2024-11-03") → ["202409", "202410", "202411"]
    """
    s = datetime.strptime(start, "%Y-%m-%d").replace(day=1)
    e = datetime.strptime(end,   "%Y-%m-%d").replace(day=1)
    if s > e:
        raise ValueError(f"start_date {start} is after end_date {end}")
    keys, cur = [], s
    while cur <= e:
        keys.append(cur.strftime("%Y%m"))
        cur += relativedelta(months=1)
    return keys


def daily_default_months() -> list[str]:
    """
    For daily job runs (no explicit date range).
    Attempts the two most recently completed months so that:
      • On the day Citi Bike publishes a new month's file the job picks it up.
      • Any month already downloaded is skipped via idempotency.
    Citi Bike typically publishes data ~2 weeks after month-end, so checking
    two months back covers the window where a new file might appear.
    """
    base = datetime.utcnow().replace(day=1)
    return [
        (base - relativedelta(months=1)).strftime("%Y%m"),  # last month
        (base - relativedelta(months=2)).strftime("%Y%m"),  # month before
    ]


if start_date_raw and end_date_raw:
    target_months = months_in_range(start_date_raw, end_date_raw)
    print(f"Mode  : date-range  ({start_date_raw} → {end_date_raw})")
elif start_date_raw or end_date_raw:
    raise ValueError("Provide both start_date and end_date, or leave both empty.")
else:
    target_months = daily_default_months()
    print("Mode  : daily default (last 2 published months)")

print(f"Months: {target_months}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3 – Download

# COMMAND ----------

def download_month(yyyymm: str) -> bool:
    """
    Download and unzip one month's CSV into the UC Volume.
    Returns True if new data was written, False if skipped.
    """
    dest_dir = os.path.join(RAW_VOLUME_PATH, yyyymm)
    os.makedirs(dest_dir, exist_ok=True)

    existing = [f for f in os.listdir(dest_dir) if f.endswith(".csv")]
    if existing:
        print(f"  [{yyyymm}] already present ({len(existing)} file(s)) — skipping")
        return False

    # Citi Bike changed their filename convention in 2021; try both.
    urls = [
        f"{BASE_URL}/{yyyymm}-citibike-tripdata.csv.zip",
        f"{BASE_URL}/{yyyymm}-citibike-tripdata.zip",
    ]
    r = None
    for url in urls:
        print(f"  [{yyyymm}] GET {url}")
        r = requests.get(url, stream=True, timeout=300)
        if r.status_code != 404:
            break
        print(f"           404 – trying next URL")

    if r.status_code == 404:
        print(f"  [{yyyymm}] not published yet — skipping")
        return False

    r.raise_for_status()

    content = b"".join(r.iter_content(chunk_size=1 << 20))
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        csv_members = [m for m in zf.namelist() if m.endswith(".csv")]
        for member in csv_members:
            zf.extract(member, dest_dir)
            print(f"           extracted → {member}")
    return True


# COMMAND ----------

print(f"\nDownloading {len(target_months)} month(s) …\n")
newly_downloaded = [ym for ym in target_months if download_month(ym)]

print(f"\n✓ Done.  New months downloaded: {newly_downloaded or 'none (all already present)'}")
display(dbutils.fs.ls(RAW_VOLUME_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4 – Preview most recent file

# COMMAND ----------

available = sorted(
    [e.name.rstrip("/") for e in dbutils.fs.ls(RAW_VOLUME_PATH)
     if e.name.rstrip("/").isdigit()],
    reverse=True,
)

if available:
    sample_path = f"{RAW_VOLUME_PATH}/{available[0]}"
    sample_df = (
        spark.read
             .option("header", "true")
             .option("inferSchema", "true")
             .csv(sample_path)
    )
    print(f"Schema for {available[0]}:")
    sample_df.printSchema()
    display(sample_df.limit(5))
else:
    print("No data in volume yet.")
