# DSCC202-402 Final Project: Tweet Sentiment Analysis Pipeline

## Overview

Build a complete end-to-end data pipeline that ingests tweets, applies ML-based sentiment analysis, and delivers analytics through an automated dashboard.

**Core Technologies**: Spark Declarative Pipelines, Delta Lake, MLflow, Unity Catalog
**Architecture**: Medallion (Bronze → Silver → Gold → Application)
**Project Weight**: 25% of final grade

---

## Pipeline Architecture

![Tweet Pipeline Architecture](tweet%20pipeline%20architecture.jpeg)

---

## Learning Objectives

Demonstrate mastery of:
1. Spark Declarative Pipelines for orchestrating data transformations
2. Medallion Architecture for organizing data processing layers
3. ML model deployment and inference at scale using Spark UDFs
4. MLflow for experiment tracking and model performance analysis
5. Data visualization and dashboard creation in Databricks
6. Workflow automation with Databricks Jobs

---

## Data Source

**Location**: `s3://dsas-datasets/tweets/`
**Format**: JSON files (~50,000 tweets)
**Schema**:
```json
{
  "date": "Mon Apr 06 22:19:49 PDT 2009",
  "user": "username123",
  "text": "@someuser This is a sample tweet!",
  "sentiment": "4"
}
```

**Test Dataset**: `s3://dsas-datasets/test-tweets/`
**Format**: JSON files (~300 tweets)
**Purpose**: Smaller dataset for bootstrapping and testing your pipeline with faster iteration

**Note**: Data is pre-provisioned. CloudFiles Auto Loader handles incremental ingestion. New data may be added daily - your automated job will process it.

---

## Architecture Requirements

### Pipeline Flow
```
S3 Bucket (Raw Tweets)
    ↓
Bronze Layer → Raw JSON ingestion
    ↓
Silver Layer → Text cleaning & mention extraction
    ↓
Gold Layer → ML sentiment predictions
    ↓
Application Layer → Aggregated metrics
    ↓
Dashboard + MLflow Tracking
```

### Bronze Layer: Raw Ingestion

**Purpose**: Ingest raw tweets from S3 using CloudFiles Auto Loader
**Input**: JSON files from `s3://dsas-datasets/tweets/`
**Output**: Delta table `tweets_bronze`

**Required Functionality**:
- Incremental ingestion with CloudFiles Auto Loader
- Schema enforcement for JSON fields (date, user, text, sentiment)
- Metadata tracking (source_file path, processing_time timestamp)
- Use Spark Declarative Pipelines API (`pyspark.pipelines`)

**Success Criteria**:
- ~50,000 tweets ingested
- All source files processed
- Metadata columns populated for all rows
- Pipeline creates Delta table with correct schema

**Reference**: See Lab 0.4 for CloudFiles Auto Loader patterns

---

### Silver Layer: Text Preprocessing

**Purpose**: Extract @mentions and clean tweet text for ML analysis
**Input**: `tweets_bronze` Delta table (streaming)
**Output**: Delta table `tweets_silver`

**Required Functionality**:
1. Extract all @mentions from tweet text using regex pattern `@[\w]+`
2. Remove @mentions from text (create `cleaned_text` column)
3. Explode mentions into individual rows (one row per mention per tweet)
4. Parse Twitter date format to proper timestamp
5. Normalize mentions to lowercase
6. Preserve tweets without mentions (use appropriate explode strategy)

**Success Criteria**:
- Row count > bronze layer (due to mention explosion)
- `cleaned_text` column has no @mentions remaining
- Tweets without mentions preserved with mention=NULL
- Timestamp properly parsed from Twitter date format
- All mentions in lowercase

**Reference**: See Lab 0.1 Section 9 for UDF patterns, Lab 0.1 Section 7 for explode operations

---

### Gold Layer: ML Inference

**Purpose**: Apply sentiment model to predict tweet sentiment
**Input**: `tweets_silver` Delta table (streaming)
**Output**: Delta table `tweets_gold`

**Required Functionality**:
1. Load sentiment model from Unity Catalog (`workspace.default.small_sentiment_model`)
2. Create Spark UDF for distributed ML inference
3. Apply model to `cleaned_text` column
4. Extract sentiment labels from model predictions (POSITIVE/NEGATIVE) and convert to lowercase
5. Extract score from predictions array and scale to 0-100 range
6. Create binary sentiment indicators: sentiment_id from ground truth (0 when sentiment='0', 1 when sentiment='4'), predicted_sentiment_id (0=negative, 1=positive)

**Success Criteria**:
- Row count matches silver layer
- Predictions present for all rows
- Confidence scores in valid range (0-100)
- Binary IDs correctly mapped (0=negative, 1=positive)
- Model loaded from Unity Catalog (not local file)

**Reference**: See Lab 0.5 for MLflow model loading and UDF creation

---

### Application Layer: Analytics Aggregations

**Purpose**: Pre-compute analytics for dashboard consumption
**Input**: `tweets_gold` Delta table
**Output**: Materialized view `gold_tweet_aggregations`

**Required Functionality**:
1. Aggregate by `mention` (lowercased username)
2. Calculate metrics per mention:
   - Count of positive mentions
   - Count of negative mentions
   - Total mentions (positive + negative)
   - Min and max timestamp (for tracking mention timeline)
3. Filter out NULL mentions
4. Sort by total mentions descending

**Success Criteria**:
- Aggregations mathematically correct
- View auto-refreshes when underlying gold table updates
- Sorted by total mentions (most mentioned users first)
- NULL mentions excluded from results

**Reference**: See Lab 0.1 Section 5 for aggregation patterns

---

## Data Schemas

### Bronze Schema
| Column | Type | Source |
|--------|------|--------|
| date | string | JSON field |
| user | string | JSON field |
| text | string | JSON field |
| sentiment | string | JSON field |
| source_file | string | _metadata.file_path |
| processing_time | timestamp | current_timestamp() |

### Silver Schema
| Column | Type | Transformation |
|--------|------|----------------|
| timestamp | timestamp | Parse date string to timestamp |
| mention | string | Extract from text using regex |
| cleaned_text | string | Remove @mentions from original text |
| text | string | Original text (unchanged) |
| sentiment | string | Original sentiment label |

### Gold Schema
| Column | Type | Source/Transformation |
|--------|------|----------------------|
| ... | ... | All silver columns passed through |
| predicted_score | double | Model confidence * 100 |
| predicted_sentiment | string | Extract and convert POSITIVE/NEGATIVE → positive/negative (lowercase) |
| sentiment_id | int | Binary ground truth: sentiment='0'→0 (negative), sentiment='4'→1 (positive) |
| predicted_sentiment_id | int | Binary prediction: 0=negative, 1=positive |

### Application Schema
| Column | Type | Aggregation |
|--------|------|-------------|
| mention | string | GROUP BY (lowercased username) |
| positive | int | COUNT WHERE predicted_sentiment = 'positive' |
| negative | int | COUNT WHERE predicted_sentiment = 'negative' |
| total | int | positive + negative |
| min_timestamp | timestamp | MIN(timestamp) |
| max_timestamp | timestamp | MAX(timestamp) |

---

## Dashboard Requirements

Create a Databricks dashboard with the following visualizations:

1. **Total Tweets Counter** - Count of all tweets from bronze layer
2. **Tweets with Mentions Counter** - Count from silver WHERE mention IS NOT NULL
3. **Tweets without Mentions Counter** - Count from silver WHERE mention IS NULL
4. **Top 10 Users by Tweet Count** - Bar chart of most active users
5. **Top 10 Most Positively Mentioned** - Bar chart (green), from application layer
6. **Top 10 Most Negatively Mentioned** - Bar chart (red), from application layer

**Deliverable**: Export dashboard as JSON and include in your repository submission.

---

## Databricks Job Requirements

Configure an automated Databricks Job with:

**Task 1: Run Pipeline**
- Type: Notebook job or DLT pipeline
- Notebook/Pipeline: Your tweet sentiment pipeline
- Schedule: Daily at 2:00 AM UTC

**Task 2: Refresh Dashboard** (depends on Task 1)
- Type: Refresh dashboard task
- Dashboard: Your tweet analytics dashboard
- Runs after Task 1 completes successfully

**Notifications**: Configure email alerts on failure

**Deliverable**: Screenshot or JSON export of job configuration showing task dependencies and schedule.

---

## Model Performance Analysis

Create an MLflow experiment that tracks:
- **Parameters**: Model name, version, data path
- **Metrics**: Classification report (precision, recall, F1 for each class)
- **Artifacts**: Confusion matrix visualization

**Required Analysis**:
1. Load `tweets_gold` with ground truth (`sentiment_id`) and predictions (`predicted_sentiment_id`)
2. Calculate classification metrics using sklearn.metrics
3. Generate confusion matrix
4. Log all to MLflow experiment
5. Register experiment in Unity Catalog

**Success Criteria**:
- MLflow experiment contains all required metrics
- Confusion matrix artifact viewable in UI
- Binary classification metrics correctly calculated
- Experiment registered and discoverable in Unity Catalog

**Reference**: See Lab 0.5 for MLflow experiment tracking patterns

---

## Implementation Approach

Your pipeline should follow this development workflow:

1. **Setup** (Run utility notebook)
   - Install dependencies
   - Load and register sentiment model in Unity Catalog
   - Verify model is accessible

2. **Implement Layers** (Use Spark Declarative Pipelines)
   - Bronze: CloudFiles streaming ingestion
   - Silver: UDF-based text processing and explode
   - Gold: Model loading and Spark UDF for inference
   - Application: SQL aggregations as materialized view

3. **Validate** (Check outputs)
   - Verify row counts at each layer
   - Sample data to verify transformations
   - Check dashboard visualizations match data

4. **Automate** (Configure job)
   - Create daily scheduled job
   - Set up task dependencies
   - Test job execution

5. **Analyze** (MLflow tracking)
   - Generate classification metrics
   - Log to MLflow experiment
   - Review model performance

---

## Grading Rubric (50 points total)

### Pipeline Execution (7 points)
- **3 pts**: Pipeline runs end-to-end without errors
- **2 pts**: All Delta tables created with correct schemas
- **2 pts**: Performance analysis notebook completes successfully

### Bronze Layer (8 points)
- **2 pts**: CloudFiles Auto Loader correctly configured
- **2 pts**: Schema enforcement implemented (4 required JSON fields)
- **2 pts**: Metadata columns present and populated
- **2 pts**: ~50,000 rows ingested successfully

### Silver Layer (10 points)
- **3 pts**: @mention extraction works correctly (regex pattern)
- **2 pts**: Text cleaning removes mentions from cleaned_text
- **2 pts**: Explode creates individual rows per mention
- **2 pts**: Date parsing converts Twitter format to timestamp
- **1 pt**: Row count > bronze due to mention explosion

### Gold Layer (12 points)
- **3 pts**: Model successfully loaded from Unity Catalog
- **3 pts**: Spark UDF applies predictions to all rows
- **2 pts**: Sentiment extraction and case conversion correct (POSITIVE/NEGATIVE → positive/negative)
- **2 pts**: Binary prediction IDs correct (negative=0, positive=1)
- **2 pts**: All required output columns present

### Application Layer (5 points)
- **1 pt**: Materialized view created
- **2 pts**: Aggregations filtered correctly (positive/negative counts)
- **2 pts**: Grouping by mention and sorting by total correct

### Model Performance Analysis (3 points)
- **1 pt**: Classification report generated and logged
- **1 pt**: Confusion matrix artifact created
- **1 pt**: MLflow experiment complete with all parameters/metrics

### Dashboard (2 points)
- **1 pt**: All 6 required visualizations present and functional
- **1 pt**: Dashboard JSON exported and included in submission

### Automated Job (3 points)
- **1 pt**: Job configured with pipeline task
- **1 pt**: Dashboard refresh task with dependency on pipeline task
- **1 pt**: Daily schedule (2 AM UTC) configured correctly

### Graduate Students Only (DSCC-402): +3 points

**Additional Requirement**: Spark Performance Optimization Analysis

Submit a 1-2 page document analyzing pipeline performance and proposing optimizations. Address at least 3 of the following:
- **Spill**: Analyze shuffle spill in aggregations, propose partition tuning
- **Skew**: Identify data skew in groupBy operations, recommend strategies
- **Shuffle**: Measure shuffle read/write, suggest repartition strategies
- **Storage**: Evaluate Delta table file sizes, recommend OPTIMIZE/ZORDER
- **Serialization**: Analyze UDF serialization overhead, propose alternatives

**Include**: Spark UI screenshots showing metrics and specific optimization recommendations with expected impact.

---

## Submission Requirements

Submit via GitHub repository with the following structure:
```
final_project/
├── tweet-pipeline/
│   ├── utilities/
│   │   └── Run me first.ipynb (provided)
│   ├── transformations/
│   │   ├── bronze_tweet_ingest.py (YOUR IMPLEMENTATION)
│   │   ├── silver_tweet_transform.py (YOUR IMPLEMENTATION)
│   │   ├── gold_tweet_transform.py (YOUR IMPLEMENTATION)
│   │   └── gold_tweet_aggregations.sql (YOUR IMPLEMENTATION)
│   ├── explorations/
│   │   └── Sentiment Model Performance Analysis.py (YOUR IMPLEMENTATION)
│   └── _dashboards/
│       └── tweet_analytics_dashboard.json (YOUR EXPORT)
├── job_configuration.json (or screenshot)
└── performance_analysis.pdf (DSCC-402 only)
```

---

## Common Issues & Troubleshooting

### CloudFiles Checkpoint Already Exists
**Error**: "Path already exists: ..."
**Solution**: Use unique checkpoint locations per pipeline run or delete old checkpoints before rerunning

### Model Not Found in Unity Catalog
**Error**: "Model 'workspace.default.small_sentiment_model' not found"
**Solution**: Verify you ran the setup utility notebook that registers the model

### Mention Explosion Missing Rows
**Error**: Tweets without mentions disappear after explode
**Solution**: Use `explode_outer()` instead of `explode()` to preserve null mentions

### Date Parsing Fails
**Error**: "Invalid format: 'Mon Apr 06 22:19:49 PDT 2009'"
**Solution**: Enable legacy time parser: `spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")`

### UDF Serialization Error
**Error**: "Task not serializable"
**Solution**: Ensure model is loaded inside UDF function, not in outer scope

### Dashboard Not Refreshing
**Issue**: Dashboard shows stale data
**Solution**: Refresh materialized view manually or configure auto-refresh on view

---

## Resources

- **Lab 0.1**: UDF creation, array operations (explode), aggregations
- **Lab 0.3**: Streaming operations, triggers, checkpoints
- **Lab 0.4**: CloudFiles Auto Loader, Delta Lake, schema evolution
- **Lab 0.5**: MLflow model loading, Spark UDFs for ML inference
- **Model**: distilbert/distilbert-base-uncased-finetuned-sst-2-english (registered as workspace.default.small_sentiment_model)
- **Spark Declarative Pipelines Docs**: https://docs.databricks.com/en/delta-live-tables/python-ref.html
- **MLflow Unity Catalog**: https://docs.databricks.com/en/mlflow/models-in-uc.html

---

## Getting Started

1. **Run the setup utility**: `final_project/tweet-pipeline/utilities/Run me first.ipynb`
2. **Implement Bronze layer**: Start with CloudFiles ingestion (use `test-tweets/` for faster initial testing)
3. **Build Silver layer**: Extract mentions and clean text
4. **Add Gold layer**: Load model and apply predictions
5. **Create Application layer**: Aggregate metrics
6. **Analyze performance**: MLflow experiment tracking
7. **Build dashboard**: 6 required visualizations
8. **Automate**: Configure Databricks Job

**Estimated Time**: 8-12 hours total

---

Good luck! Remember: This project tests your ability to apply Spark, Delta Lake, and MLflow concepts from labs to a real-world scenario. Focus on understanding the architecture and data flow - the implementation patterns are all in your lab materials.
