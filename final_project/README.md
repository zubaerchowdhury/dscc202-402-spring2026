# Tweet Sentiment Analysis Pipeline

<p align="center">
  <strong>Databricks-based medallion pipeline for ingesting tweets, running distributed sentiment inference, aggregating mention-level analytics, and serving results through an automated dashboard.</strong>
</p>

---

## Overview

This project implements an end-to-end tweet sentiment analytics workflow inside Databricks using a **Bronze → Silver → Gold → Application** architecture.

The pipeline ingests raw JSON tweets from cloud storage, cleans and restructures the text, applies a registered sentiment model at scale with Spark, aggregates mention-level metrics for downstream analysis, evaluates model performance with MLflow, and refreshes a dashboard through a scheduled Databricks Job.

Unlike a simple notebook-only sentiment project, this repository is structured like a production-style data pipeline: it separates setup, transformations, evaluation, dashboard assets, and orchestration into distinct components.

---

## Key Features

- Incremental raw tweet ingestion with **CloudFiles Auto Loader**
- Multi-layer **medallion architecture** in Databricks
- Mention extraction and cleaned-text generation using **PySpark**
- Distributed ML inference through an **MLflow pyfunc Spark UDF**
- Aggregated analytics for dashboard consumption using a **materialized view**
- Experiment tracking and artifact logging with **MLflow**
- Automated orchestration with a **Databricks Job** that runs the pipeline and refreshes the dashboard
- Repo organized into setup, transformations, evaluation, and dashboard folders for easier maintenance

---

## Data

The pipeline reads tweet data from:

- `s3://dsas-datasets/tweets/`
- `s3://dsas-datasets/test-tweets/` for faster testing / bootstrapping

### Raw schema
Each source record contains:

- `date`
- `user`
- `text`
- `sentiment`

### What the data is used for

- **`date`** is parsed into a timestamp for time-based analysis
- **`user`** preserves the original tweet author
- **`text`** is the raw input for mention extraction and sentiment inference
- **`sentiment`** is the provided label used as ground truth for evaluating predictions

This data is well suited for a medallion pipeline because it combines raw semi-structured JSON ingestion, text preprocessing, model inference, and aggregation into one reproducible workflow.

---

## Repository Structure

```text
final_project/
├── tweet-pipeline/
│   ├── utilities/
│   │   └── Run me first.py
│   ├── transformations/
│   │   ├── bronze_tweet_ingest.py
│   │   ├── silver_tweet_transform.py
│   │   ├── gold_tweet_transform.py
│   │   └── gold_tweet_aggregations.sql
│   ├── explorations/
│   │   └── Sentiment Model Performance Analysis.py
│   └── _dashboards/
│       └── tweet_analytics_dashboard.json
├── classification_report.json
├── confusion_matrix.png
├── job_configuration.json
├── Tweet-Analytics-Dashboard.png
├── Tweet-Analysis-Jobs-Runs.png
├── Tweet-Analysis-Pipeline-Runs.png
├── tweet pipeline architecture.jpeg
└── README.md
```

---

## Pipeline Architecture

### 1) Utility / environment setup
`utilities/Run me first.py` prepares the workspace by:

- creating a checkpoint volume
- dropping old working tables
- installing `transformers`, `torch`, and `torchvision`
- loading a Hugging Face sentiment model
- logging that model to MLflow
- verifying / guiding registration in Unity Catalog

This setup notebook is important because the rest of the project depends on a registered model and clean streaming state.

### 2) Bronze layer — raw ingestion
`transformations/bronze_tweet_ingest.py` creates `tweets_bronze` and reads JSON tweets from S3 with **CloudFiles Auto Loader**.

It preserves the raw source fields and adds:
- `source_file`
- `processing_time`

This layer exists to retain a trustworthy raw record of incoming data while also capturing lineage metadata.

### 3) Silver layer — text preprocessing
`transformations/silver_tweet_transform.py` creates `tweets_silver` and performs the main text-cleaning steps:

- extracts `@mentions` with regex
- removes mentions from the tweet text
- explodes mentions so there is one row per mention
- lowercases mentions for consistency
- converts the raw Twitter date string into a timestamp

This layer exists because model inference and downstream analytics work better on cleaned, standardized text than on raw source strings.

### 4) Gold layer — ML inference
`transformations/gold_tweet_transform.py` creates `tweets_gold` by loading the registered model from Unity Catalog and applying it through an **MLflow Spark UDF**.

The output adds:
- `predicted_score`
- `predicted_sentiment`
- `sentiment_id`
- `predicted_sentiment_id`

This layer turns cleaned tweets into ML-enriched analytical records that can be evaluated and aggregated at scale.

### 5) Application layer — dashboard-ready aggregation
`transformations/gold_tweet_aggregations.sql` builds the materialized view `gold_tweet_aggregations`, grouped by mentioned user.

It computes:
- positive mention count
- negative mention count
- total classified mentions
- earliest timestamp
- latest timestamp

This layer exists so the dashboard can query fast, pre-aggregated tables instead of scanning prediction-level data every time.

### 6) Evaluation and experiment tracking
`explorations/Sentiment Model Performance Analysis.py` loads the gold table, creates a classification report and confusion matrix, and logs both metrics and artifacts to MLflow.

This notebook helps validate that the deployed inference pipeline is producing reasonable results, not just outputs.

### 7) Dashboard and orchestration
- `_dashboards/tweet_analytics_dashboard.json` stores the dashboard definition
- `job_configuration.json` defines a Databricks Job that:
  1. runs the tweet analysis pipeline
  2. refreshes the dashboard afterward

This makes the project reproducible and closer to a scheduled analytics product than a one-off academic notebook.

---

## Technologies Used and Why

### Databricks
Used as the main execution environment for notebooks, pipelines, jobs, dashboards, and MLflow integration.

**Why it was used:**  
This project is fundamentally a data engineering + ML workflow, and Databricks provides the unified environment needed to handle streaming ingestion, Delta tables, model serving through Spark, experiment tracking, and job orchestration in one place.

### PySpark
Used for schema definition, streaming ingestion, transformations, UDF application, and table creation.

**Why it was used:**  
PySpark makes it possible to process large tweet datasets in a scalable, distributed way instead of relying on single-machine pandas workflows.

### Spark Declarative Pipelines (`pyspark.pipelines`)
Used to define streaming tables and append flows for the Bronze, Silver, and Gold layers.

**Why it was used:**  
It gives the pipeline a cleaner, production-style structure with explicit targets and dataflow logic.

### Delta Lake
Used as the storage/table format for the medallion layers.

**Why it was used:**  
Delta tables support reliable incremental processing, versioned data management, and structured analytics on top of streaming pipelines.

### CloudFiles Auto Loader
Used in the Bronze layer to read JSON tweets incrementally from S3.

**Why it was used:**  
It simplifies continuous or repeated ingestion from a cloud object store and is a strong fit for semi-structured JSON sources.

### MLflow
Used to log the model, create a Spark UDF from the model, track evaluation metrics, log artifacts, and tag model versions.

**Why it was used:**  
MLflow connects model management with pipeline execution, which is essential for a project that does both data engineering and model-based scoring.

### Unity Catalog
Used as the model registry target for the sentiment model.

**Why it was used:**  
Registering the model in Unity Catalog makes the model accessible and reusable from the Gold layer inference pipeline.

### Hugging Face Transformers
Used in the setup notebook to load the pretrained sentiment model.

**Why it was used:**  
It provides a strong pretrained text classification model without needing to train a custom sentiment model from scratch.

### SQL
Used for the Gold application-layer aggregation view.

**Why it was used:**  
SQL is the clearest way to express grouped dashboard metrics such as positive counts, negative counts, totals, and date ranges.

### Matplotlib + scikit-learn
Used in the evaluation notebook to generate a confusion matrix and classification report.

**Why it was used:**  
These libraries are standard, dependable tools for classification model evaluation and make the results easy to interpret.

---

## Model Used in the Actual Implementation

The implementation in this repo registers and uses:

- **Hugging Face model:** `distilbert/distilbert-base-uncased-finetuned-sst-2-english`
- **Unity Catalog model name:** `workspace.default.small_sentiment_model`

This is important because the repo’s working setup is based on a lightweight binary sentiment model that fits the Databricks pipeline flow and can be applied with an MLflow Spark UDF across the Gold layer.

---

## Results

The repository includes a saved evaluation report in `classification_report.json`.

### Logged performance
- **Accuracy:** 71.43%
- **Negative F1-score:** 0.7349
- **Positive F1-score:** 0.6903
- **Evaluated support:** 43,657 records

### Evaluation note
The evaluation notebook compares `sentiment_id` and `predicted_sentiment_id`, where the negative class is encoded as `0` and the non-negative class is encoded as `1`. In the current implementation, the deployed SST-2 model is binary, so the report effectively measures **negative vs positive** performance.

### What this means
The pipeline is not just producing predictions — it also measures how well those predictions align with the provided sentiment labels. For a course project focused on pipeline design, orchestration, and model integration, this gives the project a strong applied MLOps component instead of stopping at raw inference.

---

## Dashboard Output

The project includes dashboard assets and screenshots showing that the processed data is surfaced visually for downstream analysis.

The dashboard layer is designed to answer questions such as:
- Which mentioned users receive the most positive vs negative sentiment?
- Which accounts are mentioned most frequently?
- Over what time range were those mentions observed?

This makes the project easier to demonstrate on GitHub because it shows the full path from ingestion to business-facing analytics.

---

## Job Automation

The included Databricks job configuration automates two tasks:

1. run the tweet analysis pipeline
2. refresh the dashboard after the pipeline completes successfully

This is an important part of the implementation because it shows that the project was designed for repeatable operation rather than manual notebook execution only.

---

## How to Run

### Prerequisites
- Databricks workspace
- Access to Unity Catalog
- Access to the S3 dataset path used in the notebooks
- MLflow enabled in the workspace

### Suggested execution order

1. Run `tweet-pipeline/utilities/Run me first.py`
   - creates checkpoint storage
   - installs model dependencies
   - logs / verifies the sentiment model

2. Deploy or run the notebooks in `tweet-pipeline/transformations/`
   - Bronze ingestion
   - Silver preprocessing
   - Gold inference
   - Gold aggregation

3. Run `tweet-pipeline/explorations/Sentiment Model Performance Analysis.py`
   - generates evaluation metrics
   - logs confusion matrix and classification report to MLflow

4. Import or open `tweet-pipeline/_dashboards/tweet_analytics_dashboard.json`

5. Use `job_configuration.json` to recreate the scheduled Databricks Job if needed

---

## Why This Project Stands Out

This repository demonstrates more than sentiment classification. It shows how to:

- structure a real analytics pipeline with layered data architecture
- operationalize a pretrained NLP model in a Spark environment
- track experiments and artifacts with MLflow
- build dashboard-ready aggregations
- automate the full flow with a scheduled job

That combination makes the project stronger for showcasing **data engineering, MLOps, analytics engineering, and applied NLP** in one portfolio piece.

---

## Future Improvements

- replace the binary SST-2 model with a domain-specific or multi-class tweet sentiment model
- add richer tweet text preprocessing such as URL, hashtag, and emoji normalization
- expand dashboard metrics beyond mention-level counts
- add drift monitoring across time windows
- compare multiple registered models and track their performance over time
- parameterize dataset paths and catalog names for easier reuse across environments

---
