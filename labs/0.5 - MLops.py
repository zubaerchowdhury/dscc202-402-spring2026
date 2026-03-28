# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # MLflow Experiment Tracking and Distributed ML with Pandas UDFs
# MAGIC
# MAGIC ## Business Context
# MAGIC
# MAGIC Welcome to **NYC TaxiTech Analytics**! As a Machine Learning Engineer at MetroFleet's data science team, you're building predictive models to optimize fare pricing and trip planning. Your challenge: predict trip fares accurately while processing millions of trips efficiently.
# MAGIC
# MAGIC Key business requirements:
# MAGIC
# MAGIC - **Fare Prediction**: Build ML models to predict trip fares based on distance, time, and location
# MAGIC - **Experiment Tracking**: Systematically track model experiments, parameters, and performance
# MAGIC - **Distributed Processing**: Apply feature engineering and model inference at scale using Pandas UDFs
# MAGIC - **Model Lifecycle**: Manage model versions and compare performance across experiments
# MAGIC
# MAGIC Previously, your team trained models on small samples and applied them row-by-row, causing slow predictions and poor experiment reproducibility. You'll build a modern **MLflow + Spark ML pipeline** that enables efficient distributed processing and systematic experiment management.
# MAGIC
# MAGIC ## Dataset Overview
# MAGIC
# MAGIC You'll work with NYC Yellow Taxi trip data from `samples.nyctaxi.trips`:
# MAGIC
# MAGIC | Column | Type | Description | ML Usage |
# MAGIC |--------|------|-------------|----------|
# MAGIC | `tpep_pickup_datetime` | timestamp | Pickup time | Time-based features |
# MAGIC | `tpep_dropoff_datetime` | timestamp | Dropoff time | Trip duration calculation |
# MAGIC | `trip_distance` | double | Distance in miles | Primary predictor |
# MAGIC | `fare_amount` | double | Fare in USD | **Target variable** |
# MAGIC | `pickup_zip` | int | Pickup ZIP code | Location features |
# MAGIC | `dropoff_zip` | int | Dropoff ZIP code | Route features |
# MAGIC
# MAGIC **Sample Size**: ~100,000 trips from NYC Yellow Taxi dataset
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC This comprehensive lab combines MLflow experiment tracking with Pandas UDFs for distributed ML:
# MAGIC
# MAGIC 1. **Feature Engineering with Pandas UDFs** - Create distributed feature transformations using Pandas UDFs
# MAGIC 2. **MLflow Experiment Tracking** - Log models, parameters, metrics; compare experiments systematically
# MAGIC 3. **Distributed Inference** - Apply ML models at scale using MLflow Pandas UDFs
# MAGIC
# MAGIC **Note**: This lab is compatible with Databricks Free Edition. Some MLflow Model Registry features may have UI limitations but core functionality works!
# MAGIC
# MAGIC ## Lab Journey
# MAGIC
# MAGIC **Act 1: Feature Engineering at Scale** → Build time and distance features using Pandas UDFs
# MAGIC **Act 2: Experiment Tracking with MLflow** → Train models, log experiments, compare performance
# MAGIC **Act 3: Distributed Inference** → Apply best model at scale using MLflow Pandas UDFs
# MAGIC
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a catalog and schema for our MLflow lab
# MAGIC CREATE CATALOG IF NOT EXISTS nyctaxi_ml_catalog;
# MAGIC
# MAGIC -- Create a schema (database) in the catalog
# MAGIC CREATE SCHEMA IF NOT EXISTS nyctaxi_ml_catalog.ml_models;
# MAGIC
# MAGIC -- Create a managed volume for file storage
# MAGIC CREATE VOLUME IF NOT EXISTS nyctaxi_ml_catalog.ml_models.workspace;

# COMMAND ----------

# Set up working directory using Unity Catalog volume
import os

# Use Unity Catalog managed volume for file storage
working_dir = "/Volumes/nyctaxi_ml_catalog/ml_models/workspace"
mlflow_experiment_path = f"/Users/{spark.sql('SELECT current_user()').collect()[0][0]}/nyctaxi_ml_experiments"

print(f"Working directory: {working_dir}")
print(f"MLflow experiment path: {mlflow_experiment_path}")

# COMMAND ----------

# Clean up working directory to account for any failed previous runs
try:
    dbutils.fs.rm(f"{working_dir}/features", recurse=True)
    dbutils.fs.rm(f"{working_dir}/predictions", recurse=True)
    print(f"✅ Cleaned up working directory: {working_dir}")
except Exception as e:
    print(f"⚠️ Cleanup note: {e}")
    print("Continuing with lab...")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Important: Databricks Free Edition Compatibility
# MAGIC
# MAGIC This lab is designed for **Databricks Free Edition** with the following compatibility notes:
# MAGIC
# MAGIC ## MLflow Features:
# MAGIC - ✅ **MLflow Tracking** - Fully supported (logging params, metrics, models, artifacts)
# MAGIC - ✅ **Experiment Comparison** - Use MLflow UI and `mlflow.search_runs()` to compare experiments
# MAGIC - ⚠️ **MLflow Model Registry** - Basic features supported, but UI may have limitations in Free Edition
# MAGIC   - Model registration works
# MAGIC   - Stage transitions (None → Staging → Production) work
# MAGIC   - Model serving/deployment features may be limited
# MAGIC - ✅ **MLflow Pandas UDFs** - Fully supported for distributed inference
# MAGIC
# MAGIC ## Machine Learning:
# MAGIC - ✅ **sklearn models** - Fully supported
# MAGIC - ✅ **Pandas UDFs** - Fully supported for feature engineering and inference
# MAGIC - ✅ **Delta Lake** - All features supported for data versioning
# MAGIC
# MAGIC ## Pattern Used Throughout This Lab:
# MAGIC
# MAGIC 1. **Log experiments** using `mlflow.start_run()` and tracking APIs
# MAGIC 2. **Compare models** using MLflow search and UI
# MAGIC 3. **Apply at scale** using Pandas UDFs for distributed processing
# MAGIC 4. **Optional Model Registry** - Learn the concepts, but some features may be limited

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 1: Feature Engineering with Pandas UDFs
# MAGIC
# MAGIC **Business Goal:** Create time-based and distance-based features efficiently at scale using Pandas UDFs for millions of taxi trips.
# MAGIC
# MAGIC In this section, you'll learn how to create Pandas UDFs for distributed feature engineering. Pandas UDFs allow you to apply pandas operations across partitions of Spark DataFrames, combining the expressiveness of pandas with the scalability of Spark.
# MAGIC
# MAGIC ## Key Concepts:
# MAGIC - **Pandas UDF**: User-defined function that operates on pandas Series/DataFrames
# MAGIC - **Vectorized Operations**: Process entire batches of data efficiently
# MAGIC - **Type Hints**: Specify input/output types for Pandas UDFs
# MAGIC - **Distributed Processing**: Spark distributes UDF execution across partitions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.1: Load and Explore Data
# MAGIC
# MAGIC Load the NYC taxi trip data and examine its structure. We'll use this to build features for fare prediction.

# COMMAND ----------

# TODO
# Load NYC taxi trips and examine the data
from pyspark.sql.functions import col

# Load taxi trips
trips_df = spark.table("samples.nyctaxi.trips")

print(f"Loaded {trips_df.count():,} taxi trips")
display(trips_df.limit(10))

# COMMAND ----------

# CHECK YOUR WORK
assert trips_df.count() > 0, "Should have loaded trip data"
assert "fare_amount" in trips_df.columns, "Should have fare_amount column (target variable)"
assert "trip_distance" in trips_df.columns, "Should have trip_distance column"
print("✅ Task 1.1 complete: Data loaded successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.2: Create Time-Based Features with Pandas UDF
# MAGIC
# MAGIC Create a Pandas UDF to extract time-based features (hour of day, day of week, is_weekend) from pickup timestamps. These features help capture temporal patterns in fare pricing.
# MAGIC
# MAGIC **Pandas UDF Syntax**:
# MAGIC ```python
# MAGIC from pyspark.sql.functions import pandas_udf
# MAGIC import pandas as pd
# MAGIC
# MAGIC @pandas_udf("struct<hour:int, day_of_week:int, is_weekend:int>")
# MAGIC def extract_time_features(timestamps: pd.Series) -> pd.DataFrame:
# MAGIC     # Process pandas Series, return pandas DataFrame
# MAGIC     ...
# MAGIC ```

# COMMAND ----------

# TODO
# Create Pandas UDF to extract time features from timestamps
# Return: struct with hour (0-23), day_of_week (0=Monday, 6=Sunday), is_weekend (0 or 1)

from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf("struct<hour:int, day_of_week:int, is_weekend:int>")
def extract_time_features(timestamps: pd.Series) -> pd.DataFrame:
    """
    Extract time-based features from timestamp column.

    Args:
        timestamps: pandas Series of timestamps

    Returns:
        pandas DataFrame with columns: hour, day_of_week, is_weekend
    """
    # Convert to datetime if not already
    dt = pd.to_datetime(timestamps)

    # Extract features
    return pd.DataFrame({
        "hour": dt.dt.hour.fillna(0).astype(int), # Extract hour from dt using dt.dt.hour
        "day_of_week": dt.dt.dayofweek.fillna(0).astype(int), # Extract day of week using dt.dt.dayofweek
        "is_weekend": (dt.dt.dayofweek.fillna(0).astype(int) >= 5).astype(int) # Cast to int: (dt.dt.dayofweek >= 5).astype(int)
    })

# Apply the UDF to create time features
trips_with_time = trips_df.withColumn("time_features", extract_time_features(col("tpep_pickup_datetime")))  # Which column?

# Expand struct into individual columns
trips_with_time = trips_with_time.select(
    "*",
    col("time_features.hour").alias("pickup_hour"),
    col("time_features.day_of_week").alias("pickup_day_of_week"),
    col("time_features.is_weekend").alias("is_weekend")
).drop("time_features")

print("✅ Time features created")
display(trips_with_time.select("tpep_pickup_datetime", "pickup_hour", "pickup_day_of_week", "is_weekend").limit(5))

# COMMAND ----------

# CHECK YOUR WORK
assert "pickup_hour" in trips_with_time.columns, "Should have pickup_hour column"
assert "pickup_day_of_week" in trips_with_time.columns, "Should have day_of_week column"
assert "is_weekend" in trips_with_time.columns, "Should have is_weekend column"
# Verify hour is in valid range (0-23)
hour_range = trips_with_time.select("pickup_hour").distinct().collect()
assert all(0 <= row.pickup_hour <= 23 for row in hour_range), "Hour should be between 0 and 23"
print("✅ Task 1.2 complete: Time features created with Pandas UDF")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.3: Create Distance-Based Features with Pandas UDF
# MAGIC
# MAGIC Create a Pandas UDF to calculate trip duration (in minutes) and average speed (mph). These are critical features for fare prediction.
# MAGIC
# MAGIC **Formula**:
# MAGIC - Duration = (dropoff_time - pickup_time) in minutes
# MAGIC - Average Speed = distance / (duration in hours)

# COMMAND ----------

# TODO
# Create Pandas UDF to calculate trip_duration_minutes and avg_speed_mph

from pyspark.sql.functions import pandas_udf, struct
import pandas as pd
import numpy as np

@pandas_udf("struct<trip_duration_minutes:double, avg_speed_mph:double>")
def calculate_trip_metrics(pickup_times: pd.Series, dropoff_times: pd.Series, distances: pd.Series) -> pd.DataFrame:
    """
    Calculate trip duration and average speed.

    Args:
        pickup_times: pandas Series of pickup timestamps
        dropoff_times: pandas Series of dropoff timestamps
        distances: pandas Series of trip distances in miles

    Returns:
        pandas DataFrame with columns: trip_duration_minutes, avg_speed_mph
    """
    # Convert to datetime
    pickup = pd.to_datetime(pickup_times)
    dropoff = pd.to_datetime(dropoff_times)

    # Calculate duration in minutes
    duration_minutes = (dropoff - pickup).dt.total_seconds() / 60 # Hint: (dropoff - pickup).dt.total_seconds() / 60

    # Calculate average speed (mph) - handle division by zero
    duration_hours = duration_minutes / 60 # Convert minutes to hours
    avg_speed = np.where(duration_hours > 0, distances / duration_hours, 0) # Use np.where(duration_hours > 0, distances / duration_hours, 0)

    return pd.DataFrame({
        "trip_duration_minutes": duration_minutes,
        "avg_speed_mph": avg_speed
    })

# Apply the UDF to create trip metrics
trips_with_metrics = trips_with_time.withColumn(
    "trip_metrics",
    calculate_trip_metrics(
        col("tpep_pickup_datetime"), # pickup datetime column
        col("tpep_dropoff_datetime"), # dropoff datetime column
        col("trip_distance") # distance column
    )
)

# Expand struct into individual columns
trips_with_metrics = trips_with_metrics.select(
    "*",
    col("trip_metrics.trip_duration_minutes").alias("trip_duration_minutes"),
    col("trip_metrics.avg_speed_mph").alias("avg_speed_mph")
).drop("trip_metrics")

print("✅ Trip metrics calculated")
display(trips_with_metrics.select("trip_distance", "trip_duration_minutes", "avg_speed_mph").limit(5))

# COMMAND ----------

# CHECK YOUR WORK
assert "trip_duration_minutes" in trips_with_metrics.columns, "Should have trip_duration_minutes column"
assert "avg_speed_mph" in trips_with_metrics.columns, "Should have avg_speed_mph column"
# Check that durations are reasonable (positive, not extreme)
duration_stats = trips_with_metrics.filter(col("trip_duration_minutes") > 0).select("trip_duration_minutes").summary().collect()
print("✅ Task 1.3 complete: Distance-based features created with Pandas UDF")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.4: Save Engineered Features to Delta
# MAGIC
# MAGIC Save the feature-engineered dataset as a Delta table for use in model training. Filter out invalid records (zero/negative values).

# COMMAND ----------

# TODO
# Filter valid records and save to Delta format

# Filter out invalid records
valid_trips = trips_with_metrics.filter(
    (col("fare_amount") > 0) &  # Positive fares only
    (col("trip_distance") > 0) &  # What about distance?
    (col("trip_duration_minutes") > 0) &  # trip_duration_minutes should be positive
    (col("avg_speed_mph") > 0) &  # avg_speed_mph should be positive
    (col("avg_speed_mph") < 80)  # Remove outliers - realistic speed limit?
)

print(f"Valid trips: {valid_trips.count():,} (filtered from {trips_with_metrics.count():,})")

# Save to Delta
(valid_trips
 .write
 .format("delta")  # Delta format
 .mode("overwrite")  # overwrite mode
 .save(f"{working_dir}/features/taxi_features")  # Path: f"{working_dir}/features/taxi_features"
)

print(f"✅ Features saved to {working_dir}/features/taxi_features")

# COMMAND ----------

# CHECK YOUR WORK
features_df = spark.read.format("delta").load(f"{working_dir}/features/taxi_features")
assert features_df.count() > 0, "Should have saved features"
assert "pickup_hour" in features_df.columns, "Should have time features"
assert "trip_duration_minutes" in features_df.columns, "Should have trip metrics"
print("✅ Task 1.4 complete: Feature-engineered data saved to Delta")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 2: MLflow Experiment Tracking & Model Training
# MAGIC
# MAGIC **Business Goal:** Systematically train and track ML experiments to find the best fare prediction model, comparing different features and hyperparameters.
# MAGIC
# MAGIC In this section, you'll learn MLflow's experiment tracking capabilities: logging parameters, metrics, and models. You'll train multiple models with different feature sets and compare their performance.
# MAGIC
# MAGIC ## Key Concepts:
# MAGIC - **MLflow Tracking**: Log params, metrics, models to experiments
# MAGIC - **Experiment Comparison**: Use MLflow UI and search_runs() to compare models
# MAGIC - **Model Signatures**: Define input/output schemas for models
# MAGIC - **Artifact Logging**: Save model artifacts and metadata

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.1: Train Baseline Model with MLflow
# MAGIC
# MAGIC Train a baseline Random Forest model using only distance-based features (trip_distance, trip_duration_minutes). Log all parameters, metrics, and the model to MLflow.

# COMMAND ----------

# TODO
# Train baseline model and log to MLflow

import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import pandas as pd
import numpy as np

# Set MLflow registry URI to Unity Catalog
mlflow.set_registry_uri("databricks-uc")

# Set MLflow experiment
mlflow.set_experiment(mlflow_experiment_path)  # Use mlflow_experiment_path variable

# Load features as pandas DataFrame
features_pdf = spark.read.format("delta").load(f"{working_dir}/features/taxi_features").toPandas()

# Prepare baseline features (distance-based only)
baseline_features = ["trip_distance", "trip_duration_minutes"]
X = features_pdf[baseline_features]
y = features_pdf["fare_amount"]

# Train/test split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Start MLflow run
with mlflow.start_run(run_name="baseline_rf_model") as run:  # Use "baseline_rf_model"
    # Log parameters
    mlflow.log_param("feature_set", "baseline_distance_features")  # Log "feature_set" = "baseline_distance_features"
    mlflow.log_param("n_estimators", 50)  # Log "n_estimators" = 50
    mlflow.log_param("max_depth", 10)  # Log "max_depth" = 10

    # Train model
    rf_model = RandomForestRegressor(
        n_estimators=50,  # 50 trees
        max_depth=10,  # Depth of 10
        random_state=42
    )
    rf_model.fit(X_train, y_train)  # Which X and y to use?

    # Make predictions
    y_pred = rf_model.predict(X_test)

    # Calculate metrics
    rmse = np.sqrt(mean_squared_error(y_test, y_pred)) # np.sqrt(mean_squared_error(y_test, y_pred))
    mae = mean_absolute_error(y_test, y_pred) # mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred) # r2_score(y_test, y_pred)

    # Log metrics
    mlflow.log_metric("rmse", rmse)  # Log RMSE
    mlflow.log_metric("mae", mae)  # Log MAE
    mlflow.log_metric("r2", r2)  # Log R2

    # Log model
    mlflow.sklearn.log_model(
        rf_model, # The model object
        "model", # Artifact path: "model"
        input_example=X_train.head(5) # Example input for schema inference (e.g., X_train.head(5))
    )

    baseline_run_id = run.info.run_id

    print(f"Baseline Model - RMSE: {rmse:.2f}, MAE: {mae:.2f}, R2: {r2:.4f}")

# COMMAND ----------

# CHECK YOUR WORK
# Verify run was logged
baseline_run = mlflow.get_run(baseline_run_id)
assert "feature_set" in baseline_run.data.params, "Should have logged feature_set parameter"
assert "rmse" in baseline_run.data.metrics, "Should have logged RMSE metric"
print("✅ Task 2.1 complete: Baseline model trained and logged to MLflow")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.2: Train Improved Model with Time Features
# MAGIC
# MAGIC Train an improved model using both distance AND time-based features (pickup_hour, day_of_week, is_weekend). Log everything to MLflow for comparison.

# COMMAND ----------

# TODO
# Train improved model with time features and log to MLflow

# Prepare enhanced features (distance + time)
enhanced_features = [
    "trip_distance",
    "trip_duration_minutes", # trip_duration_minutes
    "pickup_hour", # pickup_hour
    "pickup_day_of_week", # pickup_day_of_week
    "is_weekend" # is_weekend
]
X_enhanced = features_pdf[enhanced_features]

# Train/test split
X_train_enh, X_test_enh, y_train_enh, y_test_enh = train_test_split(X_enhanced, y, test_size=0.2, random_state=42)

# Start MLflow run
with mlflow.start_run(run_name="enhanced_rf_model") as run:  # Use "enhanced_rf_model"
    # TODO: Log parameters (feature_set="enhanced_with_time_features", n_estimators=100, max_depth=15)
    mlflow.log_param("feature_set", "enhanced_with_time_features")
    mlflow.log_param("n_estimators", 100)
    mlflow.log_param("max_depth", 15)

    # TODO: Train RandomForestRegressor with n_estimators=100, max_depth=15, random_state=42
    rf_model_enh = RandomForestRegressor(
        n_estimators=100, # n_estimators
        max_depth=15, # max_depth
        random_state=42
    )
    rf_model_enh.fit(X_train_enh, y_train_enh)

    # TODO: Make predictions
    y_pred_enh = rf_model_enh.predict(X_test_enh)

    # TODO: Calculate metrics (rmse_enh, mae_enh, r2_enh)
    rmse_enh = np.sqrt(mean_squared_error(y_test_enh, y_pred_enh))
    mae_enh = mean_absolute_error(y_test_enh, y_pred_enh)
    r2_enh = r2_score(y_test_enh, y_pred_enh)

    # TODO: Log all metrics to MLflow
    mlflow.log_metric("rmse", rmse_enh)
    mlflow.log_metric("mae", mae_enh)
    mlflow.log_metric("r2", r2_enh)

    # TODO: Log the trained model with input_example
    mlflow.sklearn.log_model(
        rf_model_enh,
        "model",
        input_example=X_train_enh.head(5)
    )

    enhanced_run_id = run.info.run_id

    print(f"Enhanced Model - RMSE: {rmse_enh:.2f}, MAE: {mae_enh:.2f}, R2: {r2_enh:.4f}")
    print(f"\nImprovement over baseline:")
    print(f"  RMSE: {((rmse - rmse_enh) / rmse * 100):.2f}% better")
    print(f"  R2: {((r2_enh - r2) / r2 * 100):.2f}% better")

# COMMAND ----------

# CHECK YOUR WORK
enhanced_run = mlflow.get_run(enhanced_run_id)
assert "feature_set" in enhanced_run.data.params, "Should have logged feature_set parameter"
assert enhanced_run.data.params["feature_set"] == "enhanced_with_time_features", "Should use enhanced features"
print("✅ Task 2.2 complete: Enhanced model trained and logged to MLflow")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.3: Compare Experiments using MLflow
# MAGIC
# MAGIC Use MLflow's search_runs() API to compare all experiments and find the best performing model based on RMSE.

# COMMAND ----------

# TODO
# Search and compare all runs in the experiment

# Get experiment ID
experiment = mlflow.get_experiment_by_name(mlflow_experiment_path)  # Which experiment path?

# Search runs, ordered by RMSE (ascending = better)
all_runs = mlflow.search_runs(
    experiment_ids=[experiment.experiment_id],  # Use [experiment.experiment_id] as a list
    order_by=["metrics.rmse ASC"] # Order by metrics.rmse ASC (as a list)
)

# Display comparison
print("All Runs Comparison (sorted by RMSE):")
display(all_runs[["run_id", "params.feature_set", "metrics.rmse", "metrics.mae", "metrics.r2", "start_time"]])

# Get best run
best_run = all_runs.iloc[0] # First row from all_runs DataFrame (hint: .iloc[0])
print(f"\n🏆 Best Model:")
print(f"  Feature Set: {best_run['params.feature_set']}")
print(f"  RMSE: {best_run['metrics.rmse']:.2f}")
print(f"  R2: {best_run['metrics.r2']:.4f}")

# COMMAND ----------

# CHECK YOUR WORK
assert len(all_runs) >= 2, "Should have at least 2 runs logged"
assert "metrics.rmse" in all_runs.columns, "Should have RMSE metrics"
print("✅ Task 2.3 complete: Experiments compared using MLflow")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.4 (Optional): Register Best Model
# MAGIC
# MAGIC **Note**: This task is optional and demonstrates Model Registry concepts. In Free Edition:
# MAGIC - ✅ Model metadata registration works
# MAGIC - ❌ Artifact uploads may fail due to S3 permissions (expected behavior)
# MAGIC - ✅ You'll still learn the MLflow Model Registry API patterns
# MAGIC
# MAGIC **Important**: While Model Registry artifact uploads may fail, all model artifacts are successfully stored in MLflow Experiments (visible in the MLflow UI at your experiment path). Section 3 will work perfectly because it loads models directly from experiment runs using `runs:/{run_id}/model` URIs, not from the Model Registry.
# MAGIC
# MAGIC Register the best performing model to MLflow Model Registry and transition it to "Staging" stage.

# COMMAND ----------

# TODO (Optional)
# Register the best model to Model Registry

model_name = f"nyctaxi_fare_predictor_{spark.sql('SELECT current_user()').collect()[0][0].split('@')[0]}"

# Get best model URI
best_model_uri = f"runs:/{best_run['run_id']}/model"  # Use best_run['run_id']

try:
    # Register model
    model_version = mlflow.register_model(
        model_uri=best_model_uri,  # The model URI
        name= model_name # The model name
    )

    print(f"✅ Model registered: {model_name} (version {model_version.version})")

    # Transition to Staging
    from mlflow.tracking.client import MlflowClient
    client = MlflowClient()

    client.transition_model_version_stage(
        name=model_name,
        version=model_version.version,
        stage="Staging"
    )

    print(f"✅ Model transitioned to Staging stage")

except Exception as e:
    print(f"⚠️ Model Registry Limitation in Free Edition:")
    print(f"   Model metadata registered successfully, but artifact uploads failed due to S3 permissions.")
    print(f"   This is expected in Free Edition - you've learned the concepts!")
    print(f"   Error details: {str(e)[:200]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Section 3: Distributed Inference with Pandas UDFs
# MAGIC
# MAGIC **Business Goal:** Apply the best ML model to millions of trips efficiently using Pandas UDFs for distributed inference.
# MAGIC
# MAGIC In this section, you'll learn how to load an MLflow model and create a custom Pandas UDF for distributed inference. The model is loaded once and reused across predictions, which is much more efficient than loading the model for each row.
# MAGIC
# MAGIC ## Key Concepts:
# MAGIC - **mlflow.pyfunc.load_model()**: Loads MLflow model for inference
# MAGIC - **Custom Pandas UDF**: Creates distributed UDF compatible with Free Edition serverless mode
# MAGIC - **Model Reuse**: Model loaded once and reused for all predictions
# MAGIC - **Distributed Inference**: Predictions computed in parallel across cluster
# MAGIC - **Performance**: Significantly faster than row-by-row inference

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.1: Load Model and Create Pandas UDF for Inference
# MAGIC
# MAGIC Load the best model from MLflow and create a custom Pandas UDF for distributed inference.
# MAGIC
# MAGIC **Note**: In Databricks Free Edition serverless mode, `mlflow.pyfunc.spark_udf()` has limitations with Spark Connect. We'll use a manual Pandas UDF approach that loads the model explicitly, which works reliably in all environments.

# COMMAND ----------

# TODO
# Load model and create manual Pandas UDF for distributed inference

import mlflow.pyfunc
from pyspark.sql.functions import pandas_udf, PandasUDFType
import pandas as pd

# Get the best model URI and load the model
best_model_uri = f"runs:/{best_run['run_id']}/model"  # Use best_run['run_id'] from Task 2.3
loaded_model = mlflow.pyfunc.load_model(best_model_uri)  # Load the model URI

print(f"✅ Loaded model from: {best_model_uri}")

# Create Pandas UDF that uses the loaded model
@pandas_udf("double")
def predict_fare_udf(trip_distance: pd.Series, trip_duration_minutes: pd.Series,
                      pickup_hour: pd.Series, pickup_day_of_week: pd.Series,
                      is_weekend: pd.Series) -> pd.Series:
    """
    Pandas UDF for distributed fare prediction.
    Takes individual feature columns and returns predictions.
    """
    # TODO: Construct feature DataFrame matching model's expected input
    # Create DataFrame with columns matching enhanced_features from Task 2.2
    features_df = pd.DataFrame({
        "trip_distance": trip_distance,  # trip_distance
        "trip_duration_minutes": trip_duration_minutes,  # trip_duration_minutes
        "pickup_hour": pickup_hour,  # pickup_hour
        "pickup_day_of_week": pickup_day_of_week,  # pickup_day_of_week
        "is_weekend": is_weekend # is_weekend
    })

    # TODO: Make predictions using the loaded model
    predictions = loaded_model.predict(features_df) # Use loaded_model.predict(...)

    return pd.Series(predictions)  # Return predictions as Series

print("✅ Created Pandas UDF for distributed inference")

# COMMAND ----------

# CHECK YOUR WORK
assert loaded_model is not None, "Should have loaded model"
assert predict_fare_udf is not None, "Should have created prediction UDF"
print("✅ Task 3.1 complete: Model loaded and Pandas UDF created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.2: Apply Predictions at Scale
# MAGIC
# MAGIC Apply the prediction UDF to the entire dataset using the enhanced features. The model will be distributed across partitions for efficient inference.

# COMMAND ----------

# TODO
# Apply predictions using the Pandas UDF

from pyspark.sql.functions import abs

# Load feature-engineered data
features_spark_df = spark.read.format("delta").load(f"{working_dir}/features/taxi_features")

# Apply predictions using UDF with individual feature columns
predictions_df = features_spark_df.withColumn(
    "predicted_fare",
    predict_fare_udf(
        col("trip_distance"),  # trip_distance
        col("trip_duration_minutes"),  # trip_duration_minutes
        col("pickup_hour"),  # pickup_hour
        col("pickup_day_of_week"),  # pickup_day_of_week
        col("is_weekend")   # is_weekend
    )
)

# Calculate prediction error
predictions_df = predictions_df.withColumn(
    "prediction_error",
    col("predicted_fare") - col("fare_amount")  # predicted - actual
).withColumn(
    "absolute_error",
    abs(col("prediction_error"))  # Absolute value of prediction_error
).withColumn(
    "percentage_error",
    (col("absolute_error") / col("fare_amount")) * 100  # (absolute_error / fare_amount) * 100
)

print(f"✅ Generated predictions for {predictions_df.count():,} trips")
display(predictions_df.select("fare_amount", "predicted_fare", "absolute_error", "percentage_error").limit(10))

# COMMAND ----------

# CHECK YOUR WORK
assert "predicted_fare" in predictions_df.columns, "Should have predicted_fare column"
assert "prediction_error" in predictions_df.columns, "Should have prediction_error column"
print("✅ Task 3.2 complete: Predictions applied at scale using Pandas UDF")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.3: Analyze Prediction Performance
# MAGIC
# MAGIC Calculate summary statistics on prediction errors to understand model performance in production.

# COMMAND ----------

# TODO
# Analyze prediction performance

from pyspark.sql.functions import mean, stddev, percentile_approx, count

# TODO: Calculate prediction statistics
# Use aggregation functions: mean, stddev, percentile_approx, count

prediction_stats = predictions_df.select(
    mean("absolute_error").alias("mean_absolute_error"),  # Mean absolute error
    stddev( "absolute_error" ).alias("std_absolute_error"),  # Std of absolute error
    mean("percentage_error").alias("mean_percentage_error"),  # Mean percentage error
    percentile_approx("absolute_error" , 0.5).alias("median_absolute_error"),  # Median absolute error
    percentile_approx("absolute_error" , 0.95).alias("p95_absolute_error"),  # 95th percentile
    count("*").alias("total_predictions")  # Total predictions
).collect()[0]

print("Prediction Performance Summary:")
print(f"  Total Predictions: {prediction_stats['total_predictions']:,}")
print(f"  Mean Absolute Error: ${prediction_stats['mean_absolute_error']:.2f}")
print(f"  Median Absolute Error: ${prediction_stats['median_absolute_error']:.2f}")
print(f"  95th Percentile Error: ${prediction_stats['p95_absolute_error']:.2f}")
print(f"  Mean Percentage Error: {prediction_stats['mean_percentage_error']:.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.4: Save Predictions to Delta
# MAGIC
# MAGIC Save the predictions to Delta format for downstream analytics and monitoring.

# COMMAND ----------

# TODO
# Save predictions to Delta table

# Select relevant columns for storage
predictions_to_save = predictions_df.select(
    "tpep_pickup_datetime",  # tpep_pickup_datetime
    "tpep_dropoff_datetime",  # tpep_dropoff_datetime
    "trip_distance",  # trip_distance
    "pickup_zip",  # pickup_zip
    "dropoff_zip",  # dropoff_zip
    "fare_amount",  # fare_amount
    "predicted_fare",  # predicted_fare
    "prediction_error",  # prediction_error
    "absolute_error",  # absolute_error
    "percentage_error" # percentage_error
)

# Save to Delta
(predictions_to_save
 .write
 .format("delta")  # Delta format
 .mode("overwrite")  # overwrite mode
 .save(f"{working_dir}/predictions/fare_predictions")  # Path: f"{working_dir}/predictions/fare_predictions"
)

print(f"✅ Predictions saved to {working_dir}/predictions/fare_predictions")

# COMMAND ----------

# CHECK YOUR WORK
predictions_check = spark.read.format("delta").load(f"{working_dir}/predictions/fare_predictions")
assert predictions_check.count() > 0, "Should have saved predictions"
assert "predicted_fare" in predictions_check.columns, "Should have prediction columns"
print("✅ Task 3.4 complete: Predictions saved to Delta")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Conclusion
# MAGIC
# MAGIC Congratulations! You've built a complete **MLflow + Pandas UDF pipeline** for distributed machine learning:
# MAGIC
# MAGIC ## What You've Accomplished:
# MAGIC
# MAGIC ✅ **Feature Engineering at Scale** - Created time and distance features using Pandas UDFs
# MAGIC ✅ **MLflow Experiment Tracking** - Logged models, parameters, metrics systematically
# MAGIC ✅ **Model Comparison** - Used MLflow to compare experiments and select best model
# MAGIC ✅ **Distributed Inference** - Applied predictions at scale using MLflow Pandas UDFs
# MAGIC ✅ **Optional: Model Registry** - Learned model lifecycle management concepts
# MAGIC
# MAGIC ## Key Takeaways:
# MAGIC
# MAGIC 1. **Pandas UDFs = Scale + Simplicity** - Write pandas code, get Spark scalability
# MAGIC 2. **MLflow Tracking = Reproducibility** - Every experiment is logged and comparable
# MAGIC 3. **Experiment Comparison** - Use search_runs() to find best models systematically
# MAGIC 4. **MLflow Pandas UDF = Efficient Inference** - Load model once per partition, not per row
# MAGIC 5. **Model Registry** - Manage model lifecycle (Staging → Production)
# MAGIC 6. **Delta Integration** - Versioned data + versioned models = full reproducibility
# MAGIC 7. **Free Edition Compatible** - Core ML workflows work in Free Edition
# MAGIC
# MAGIC ## Production Considerations:
# MAGIC
# MAGIC - **Feature Store**: In production, use Databricks Feature Store for centralized feature management
# MAGIC - **Model Monitoring**: Track prediction drift and data quality over time
# MAGIC - **A/B Testing**: Compare production models using MLflow Model Registry stages
# MAGIC - **Batch vs Streaming**: This lab used batch; same Pandas UDFs work for streaming inference
# MAGIC - **Model Serving**: Full model serving available in Databricks workspace (may be limited in Free Edition)
# MAGIC
# MAGIC You've mastered distributed ML with MLflow and Pandas UDFs - essential skills for production ML engineering!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# Optional: Clean up lab data (uncomment to execute)
dbutils.fs.rm(f"{working_dir}/features", recurse=True)
dbutils.fs.rm(f"{working_dir}/predictions", recurse=True)
print("✅ Lab cleanup complete")
