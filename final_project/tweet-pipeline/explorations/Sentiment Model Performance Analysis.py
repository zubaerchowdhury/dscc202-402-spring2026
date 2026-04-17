# Databricks notebook source
# MAGIC %md
# MAGIC # Sentiment Model Performance Analysis
# MAGIC
# MAGIC ## Purpose
# MAGIC Evaluate sentiment classification model performance by comparing predictions against ground truth.
# MAGIC Generate classification metrics and log results to MLflow for experiment tracking.
# MAGIC
# MAGIC ## Requirements
# MAGIC - Load tweets_gold table with predictions and ground truth
# MAGIC - Calculate classification metrics (accuracy, precision, recall, F1)
# MAGIC - Generate confusion matrix visualization
# MAGIC - Log metrics, parameters, and artifacts to MLflow
# MAGIC
# MAGIC ## Expected Output
# MAGIC - Classification report with per-class metrics
# MAGIC - Confusion matrix visualization
# MAGIC - MLflow experiment with accuracy metric and confusion matrix artifact
# MAGIC
# MAGIC ## Reference
# MAGIC See Lab 0.5 (MLops) for MLflow experiment tracking patterns

# COMMAND ----------

# TODO: Import necessary libraries
# You will need:
# - pyspark.sql functions
# - pandas
# - mlflow and MlflowClient
# - delta.tables.DeltaTable
# - matplotlib.pyplot
# - sklearn.metrics (confusion_matrix, classification_report, ConfusionMatrixDisplay)
import pandas as pd
import mlflow
from mlflow import MlflowClient
from delta.tables import DeltaTable
import matplotlib.pyplot as plt
from pyspark.sql.functions import col
from sklearn.metrics import confusion_matrix, classification_report, ConfusionMatrixDisplay

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: Load Gold Data
# MAGIC
# MAGIC TODO: Read the tweets_gold table to get predicted and actual sentiments
# MAGIC - Load table using spark.read.format("delta").table()
# MAGIC - Table contains sentiment_id (ground truth) and predicted_sentiment_id (model prediction)
# MAGIC - Both are binary: 0=negative, 1=positive/neutral

# COMMAND ----------

# TODO: Load gold table
gold_df = spark.read.format("delta").table("workspace.default.tweets_gold").select(
    "sentiment_id", "predicted_sentiment_id"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Generate Classification Report
# MAGIC
# MAGIC TODO: Convert to pandas and compute classification metrics
# MAGIC 1. Convert gold DataFrame to pandas using .toPandas()
# MAGIC 2. Extract y_true from sentiment_id column
# MAGIC 3. Extract y_pred from predicted_sentiment_id column
# MAGIC 4. Define target_names as ["Negative", "Positive"]
# MAGIC 5. Generate classification_report with output_dict=True
# MAGIC
# MAGIC Reference: sklearn.metrics.classification_report

# COMMAND ----------

# TODO: Generate classification report
gold_pdf = gold_df.toPandas()
y_true = gold_pdf["sentiment_id"]
y_pred = gold_pdf["predicted_sentiment_id"]
target_names = ["Negative", "Positive"]

report = classification_report(y_true, y_pred, target_names=target_names, output_dict=True)
print(classification_report(y_true, y_pred, target_names=target_names))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Create Confusion Matrix
# MAGIC
# MAGIC TODO: Visualize model performance with confusion matrix
# MAGIC 1. Generate confusion matrix using sklearn.metrics.confusion_matrix
# MAGIC 2. Create ConfusionMatrixDisplay with target names
# MAGIC 3. Plot and display the matrix
# MAGIC
# MAGIC Confusion Matrix Layout:
# MAGIC                Predicted
# MAGIC              Neg    Pos
# MAGIC Actual  Neg   TN     FP
# MAGIC        Pos   FN     TP

# COMMAND ----------

# TODO: Create and display confusion matrix
cm = confusion_matrix(y_true, y_pred)
disp = ConfusionMatrixDisplay(confusion_matrix=cm, display_labels=target_names)
fig, ax = plt.subplots(figsize=(6, 5))
disp.plot(ax=ax, cmap="Blues", values_format="d")
ax.set_title("Tweet Sentiment Confusion Matrix")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4: Log Results to MLflow
# MAGIC
# MAGIC TODO: Track model performance in MLflow experiment
# MAGIC 1. Set MLflow registry to Unity Catalog: mlflow.set_registry_uri("databricks-uc")
# MAGIC 2. Get Delta table version from tweets_silver (for data lineage)
# MAGIC 3. Start MLflow run
# MAGIC 4. Log metrics:
# MAGIC    - accuracy from classification report
# MAGIC 5. Log parameters:
# MAGIC    - model_name: "workspace.default.tweet_sentiment_model"
# MAGIC    - model_version: 1
# MAGIC    - silver_delta_version: from Delta table history
# MAGIC 6. Log artifact:
# MAGIC    - confusion matrix figure as "confusion_matrix.png"
# MAGIC
# MAGIC Reference: Lab 0.5 for MLflow logging patterns

# COMMAND ----------

# TODO: Log metrics and artifacts to MLflow
mlflow.set_registry_uri("databricks-uc")

silver_delta = DeltaTable.forName(spark, "workspace.default.tweets_silver")
silver_delta_version = silver_delta.history(1).select("version").collect()[0][0]

with mlflow.start_run(run_name="tweet_sentiment_performance_analysis") as run:
    mlflow.log_metric("accuracy", report["accuracy"])

    mlflow.log_param("model_name", "workspace.default.tweet_sentiment_model")
    mlflow.log_param("model_version", 1)
    mlflow.log_param("silver_delta_version", silver_delta_version)

    mlflow.log_figure(fig, "confusion_matrix.png")

    print(f"Logged MLflow run: {run.info.run_id}")
    print(f"Accuracy: {report['accuracy']:.4f}")
    print(f"Silver Delta version: {silver_delta_version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC
# MAGIC After running this notebook, verify in the MLflow UI:
# MAGIC 1. Navigate to "Experiments" tab
# MAGIC 2. Find experiment for this notebook
# MAGIC 3. Check latest run contains:
# MAGIC    - accuracy metric (e.g., 0.85 = 85% correct)
# MAGIC    - model_name, model_version, silver_delta_version parameters
# MAGIC    - confusion_matrix.png artifact
# MAGIC
# MAGIC ## Interpreting Results
# MAGIC
# MAGIC **Accuracy**:
# MAGIC - High (>80%): Model performing well
# MAGIC - Low (<70%): Consider different model or fine-tuning
# MAGIC
# MAGIC **Confusion Matrix**:
# MAGIC - Diagonal (TN, TP): Correct predictions
# MAGIC - Off-diagonal (FP, FN): Misclassifications
# MAGIC - Imbalanced: May indicate class imbalance or bias
# MAGIC
# MAGIC **Next Steps**:
# MAGIC - If accuracy low: Try different model, improve preprocessing
# MAGIC - If confusion matrix shows bias: Investigate class distribution, confidence thresholds
