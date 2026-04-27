#!/bin/bash

# DSAS 2025 - GitHub Codespace Startup Script
# Initializes the workspace and starts Jupyter Lab

set -e  # Exit on any error

echo "🚀 DSAS 2025 - Initializing Codespace Learning Environment"
echo "=========================================================="

# Set up environment variables (in case they're not loaded)
export SPARK_HOME=/opt/spark
export JAVA_HOME=/usr/local/sdkman/candidates/java/current
export PATH=$JAVA_HOME/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
export PYSPARK_PYTHON=/usr/local/bin/python3
export PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH
export SPARK_LOCAL_IP=127.0.0.1
export SPARK_DRIVER_HOST=127.0.0.1

# Check if we're in the right directory
WORKSPACE_DIR="/workspaces/dscc202-402-spring2026"
if [ ! -d "$WORKSPACE_DIR" ]; then
    echo "⚠️  Warning: Expected workspace directory not found. Using current directory."
    WORKSPACE_DIR=$(pwd)
fi

cd "$WORKSPACE_DIR"

# Ensure student workspace directory exists
echo "📁 Setting up student workspace..."
mkdir -p "Course Content/Module 1 - Foundation/Lab Notebooks/student-work"

# Check lab materials are present
if [ -d "Course Content/Module 1 - Foundation/Lab Notebooks/Student Versions" ]; then
    echo "   ✓ Student notebooks ready in Course Content/Module 1 - Foundation/Lab Notebooks/Student Versions/"
    echo "   ✓ Solution notebooks available in Course Content/Module 1 - Foundation/Lab Notebooks/Solution Versions/"
    echo "   ✓ Datasets available in Course Content/Module 1 - Foundation/Lab Notebooks/Datasets/"
    echo "   ✓ Student workspace ready in Course Content/Module 1 - Foundation/Lab Notebooks/student-work/"
else
    echo "   ⚠️  Warning: Lab notebooks not found in expected location"
fi

# Create a quick experiment notebook for students
if [ ! -f "Course Content/Module 1 - Foundation/Lab Notebooks/student-work/My-Spark-Experiments.ipynb" ]; then
    echo "🧪 Creating experiment notebook template..."

    cat > "Course Content/Module 1 - Foundation/Lab Notebooks/student-work/My-Spark-Experiments.ipynb" << 'EOF'
{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "# My Spark Experiments\n",
    "\n",
    "Use this notebook for your own Spark explorations and experiments!\n",
    "\n",
    "## Quick Start Template"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "from pyspark.sql import SparkSession\n",
    "import pandas as pd\n",
    "import numpy as np\n",
    "\n",
    "# Initialize Spark Session\n",
    "spark = SparkSession.builder \\\n",
    "    .appName(\"My-DSAS-Experiments\") \\\n",
    "    .config(\"spark.sql.adaptive.enabled\", \"true\") \\\n",
    "    .getOrCreate()\n",
    "\n",
    "print(f\"✓ Spark {spark.version} ready!\")\n",
    "print(f\"📊 Spark UI: {spark.sparkContext.uiWebUrl}\")\n",
    "print(f\"💾 Available datasets: ../datasets/\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Load a sample dataset\n",
    "df = spark.read.csv(\"../datasets/customers.csv\", header=True, inferSchema=True)\n",
    "df.show(5)\n",
    "df.printSchema()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Your experiments here!\n",
    "# Try different Spark operations, explore the datasets, test new concepts\n"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.11.0"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 4
}
EOF

    echo "   ✓ Created experiment template notebook"
fi

# Test Spark installation quickly
echo "🧪 Verifying Spark installation..."
python3 -c "
import sys
import os
sys.path.insert(0, '/opt/spark/python')
sys.path.insert(0, '/opt/spark/python/lib/py4j-0.10.9.7-src.zip')

try:
    from pyspark.sql import SparkSession
    print('   ✓ PySpark import successful')
except ImportError as e:
    print(f'   ❌ PySpark import failed: {e}')
    sys.exit(1)
"

# Check if Jupyter is already running
if pgrep -f "jupyter" > /dev/null; then
    echo "📓 Jupyter Lab is already running"
    echo "   🌐 Access at: http://localhost:8888"
else
    echo "📓 Starting Jupyter Lab..."

    # Navigate to repository root for Jupyter
    cd "$WORKSPACE_DIR"

    # Start Jupyter Lab in the background
    nohup jupyter lab \
        --ip=0.0.0.0 \
        --port=8888 \
        --no-browser \
        --allow-root \
        --ServerApp.token='' \
        --ServerApp.password='' \
        --ServerApp.allow_origin='*' \
        --ServerApp.base_url='/' \
        --ServerApp.authenticate_prometheus=False > /tmp/jupyter.log 2>&1 &

    # Wait a moment for Jupyter to start
    sleep 3

    if pgrep -f "jupyter" > /dev/null; then
        echo "   ✓ Jupyter Lab started successfully"
        echo "   🌐 Access at: http://localhost:8888"
    else
        echo "   ⚠️  Jupyter Lab may have issues starting. Check /tmp/jupyter.log"
    fi
fi

# Display useful information
echo ""
echo "✅ DSAS 2025 Codespace Ready!"
echo "=============================="
echo ""
echo "🎯 Quick Start:"
echo "   1. Open: http://localhost:8888 (Jupyter Lab)"
echo "   2. Run: Environment-Check.ipynb to verify setup"
echo "   3. Start: Course Content/Module 1 - Foundation/Lab Notebooks/Student Versions/Lab 1 - RDD Fundamentals.ipynb"
echo ""
echo "🔧 Spark Details:"
echo "   • Version: 3.5.0"
echo "   • UI: http://localhost:4040 (when Spark is running)"
echo "   • Memory: 3GB driver + 2GB executor"
echo "   • Cores: 2 + 2"
echo ""
echo "📁 Workspace Structure:"
echo "   • Labs: Course Content/Module 1 - Foundation/Lab Notebooks/Student Versions/"
echo "   • Datasets: Course Content/Module 1 - Foundation/Lab Notebooks/Datasets/"
echo "   • Solutions: Course Content/Module 1 - Foundation/Lab Notebooks/Solution Versions/"
echo "   • Experiments: Course Content/Module 1 - Foundation/Lab Notebooks/student-work/"
echo ""
echo "🚀 Happy Learning!"
echo ""