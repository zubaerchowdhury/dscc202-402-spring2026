# Databricks notebook source
# MAGIC %sql
# MAGIC DROP VOLUME IF EXISTS workspace.default.checkpoints;
# MAGIC
# MAGIC -- create the checkpoint volume for reading the raw tweet stream
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.default.checkpoints;
# MAGIC
# MAGIC DROP TABLE IF EXISTS workspace.default.tweets_bronze;
# MAGIC DROP TABLE IF EXISTS workspace.default.tweets_silver;
# MAGIC DROP TABLE IF EXISTS workspace.default.tweets_gold;
# MAGIC DROP TABLE IF EXISTS workspace.default.gold_tweet_agg;
# MAGIC
# MAGIC

# COMMAND ----------

# Install transformers, torch, and torchvision (required for Hugging Face models)
%pip install transformers==4.35.2 torch torchvision --quiet
dbutils.library.restartPython()

# COMMAND ----------

import mlflow
from mlflow import MlflowClient
from transformers import AutoTokenizer, AutoModelForSequenceClassification

# Use Unity Catalog Model Registry
# Note: Free Edition has limited permissions - model registration must be done via UI (one-time setup)
mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

# Define model details
HF_MODEL_NAME = "distilbert/distilbert-base-uncased-finetuned-sst-2-english"
UC_MODEL_NAME = "workspace.default.small_sentiment_model"

print(f"🤗 Loading Hugging Face model: {HF_MODEL_NAME}")
print(f"   This may take a few minutes on first download...")

# Load model and tokenizer from Hugging Face
tokenizer = AutoTokenizer.from_pretrained(HF_MODEL_NAME)
model = AutoModelForSequenceClassification.from_pretrained(HF_MODEL_NAME)

print(f"✅ Model loaded successfully!")
print(f"   Model size: ~67M parameters")
print(f"   Output classes: 2 (negative, positive)")

# COMMAND ----------

# Log model to MLflow run storage (works in Free Edition)
# Note: Model REGISTRATION must be done via UI in Free Edition (see instructions below)
print(f"📦 Logging model to MLflow with transformers flavor...")

with mlflow.start_run(run_name="tweet_sentiment_hf_model") as run:
    # Log the model using transformers flavor
    model_info = mlflow.transformers.log_model(
        transformers_model={
            "model": model,
            "tokenizer": tokenizer
        },
        artifact_path="model",
        input_example=["This is a great day!"],  # Example for schema inference
        task="text-classification"
    )

    # Log model metadata for reference
    mlflow.log_param("hf_model_name", HF_MODEL_NAME)
    mlflow.log_param("task", "sentiment-classification")
    mlflow.log_param("num_labels", 2)
    mlflow.log_param("model_type", "distillbert-base")

    run_id = run.info.run_id
    model_uri = f"runs:/{run_id}/model"

    print(f"\n✅ Model logged to MLflow run: {run_id}")
    print(f"   Model URI: {model_uri}")

# COMMAND ----------

# Free Edition requires manual registration via Databricks UI
print(f"\n📋 ONE-TIME SETUP: Register Model via Databricks UI")
print(f"=" * 70)
print(f"\n⚠️  IMPORTANT: Only do this ONCE when first setting up the lab!")
print(f"   If model already registered, skip to verification below.\n")
print(f"📝 Manual Registration Steps:")
print(f"   1. In Databricks workspace, click 'Machine Learning' in left sidebar")
print(f"   2. Click 'Experiments' tab")
print(f"   3. Find and click the experiment containing run: {run_id}")
print(f"   4. Click the run to open run details")
print(f"   5. Scroll down to 'Artifacts' section → click 'model' folder")
print(f"   6. Click 'Register Model' button (top right)")
print(f"   7. In dialog:")
print(f"      - Model: Select 'Create New Model'")
print(f"      - Model Name: {UC_MODEL_NAME}")
print(f"      - Click 'Register'")
print(f"\n   ✅ Registration complete! Proceed to verification below.")
print(f"=" * 70)

# COMMAND ----------

# Verify model is registered in Unity Catalog
client = MlflowClient()
try:
    model_versions = client.search_model_versions(f"name='{UC_MODEL_NAME}'")
    #for mv in model_versions:
    #    print(f"Version: {mv.version}, Status: {mv.status}, Description: {mv.description}")

    print(f"\n✅ Model registered successfully in Unity Catalog!")
    print(f"   Name: {model_versions[0].name}")
    print(f"   Description: {model_versions[0].description or 'N/A'}")

    if model_versions[0].version:
        print(f"   Latest version: {model_versions[0].version}")
        print(f"   Status: {model_versions[0].status}")
        print(f"\n   Model URI: models:/{UC_MODEL_NAME}/{model_versions[0].version}")
    else:
        print(f"   ⚠️  No versions found - complete manual registration above!")

except Exception as e:
    print(f"❌ Model not found: {e}")
    print(f"\n⚠️  Please complete the ONE-TIME manual registration above!")
    print(f"   Follow the UI registration steps, then rerun this cell to verify.")

# COMMAND ----------

#Load the model from the URI above and execute a test inference 
pyfunc_model = mlflow.pyfunc.load_model(model_uri)

# The model is logged with an input example
input_data = pyfunc_model.input_example

# Verify the model with the provided input data using the logged dependencies.
# For more details, refer to:
# https://mlflow.org/docs/latest/models.html#validate-models-before-deployment
mlflow.models.predict(
    model_uri=model_uri,
    input_data=input_data,
    env_manager="uv",
)
