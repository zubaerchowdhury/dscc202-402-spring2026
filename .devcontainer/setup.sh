#!/bin/bash

# DSAS 2025 - GitHub Codespace Setup Script
# Installs Apache Spark and configures the learning environment

set -e  # Exit on any error

echo "🚀 DSAS 2025 - Setting up Apache Spark Learning Environment"
echo "============================================================="

# Update system packages
echo "📦 Updating system packages..."
sudo apt-get update && sudo apt-get upgrade -y

# Install additional system dependencies
echo "🔧 Installing system dependencies..."
sudo apt-get install -y \
    curl \
    wget \
    htop \
    vim \
    tree \
    unzip \
    build-essential

# Set Spark version
SPARK_VERSION="3.5.0"
HADOOP_VERSION="3"
SPARK_PACKAGE="spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}"
SPARK_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_PACKAGE}.tgz"

# Download and install Apache Spark
echo "⚡ Installing Apache Spark ${SPARK_VERSION}..."

# Create Spark directory
sudo mkdir -p /opt/spark

# Download Spark if not already present
if [ ! -f "/tmp/${SPARK_PACKAGE}.tgz" ]; then
    echo "   📥 Downloading Spark from ${SPARK_URL}"
    wget -q -O "/tmp/${SPARK_PACKAGE}.tgz" "${SPARK_URL}"
else
    echo "   ✓ Spark package already downloaded"
fi

# Extract Spark
echo "   📂 Extracting Spark..."
sudo tar -xzf "/tmp/${SPARK_PACKAGE}.tgz" -C /opt/
sudo mv "/opt/${SPARK_PACKAGE}"/* /opt/spark/
sudo rm -rf "/opt/${SPARK_PACKAGE}"

# Set proper permissions
sudo chown -R vscode:vscode /opt/spark

# Install Python dependencies
echo "🐍 Installing Python packages for data science..."

# Create requirements file matching UV pyproject.toml configuration
cat > /tmp/dsas_requirements.txt << 'EOF'
# Core Data Science Stack (Updated to match UV local environment)
pandas>=2.3.2
numpy>=2.3.3
matplotlib>=3.8.0
seaborn>=0.13.0
plotly>=5.17.0

# Jupyter Core & Extensions
jupyter>=1.1.1
jupyterlab>=4.0.9
notebook>=7.0.0
jupyterlab-git>=0.50.0
jupyter-dash>=0.4.2
nbconvert>=7.12.0

# Big Data & Spark (Updated to match UV configuration)
pyspark>=3.5.0
py4j>=0.10.9.7
pyarrow>=21.0.0

# Performance and Memory Optimization (Added from UV config)
psutil>=6.1.0
memory-profiler>=0.61.0

# Additional Utilities
requests>=2.31.0
beautifulsoup4>=4.12.2
lxml>=4.9.3

# Development Tools (Updated versions)
ipykernel>=6.29.0
ipywidgets>=8.1.1
tqdm>=4.66.1
EOF

# Install Python packages
pip install --upgrade pip

# Install core Jupyter packages globally first
pip install "jupyterlab>=4.0.9" "notebook>=7.0.0"

# Install remaining packages (dependencies synchronized with UV pyproject.toml)
pip install -r /tmp/dsas_requirements.txt

# Verify critical packages for Labs 6-7 are installed (Updated to UV versions)
echo "🔍 Verifying critical package installations..."
pip install --upgrade "pandas>=2.3.2" "numpy>=2.3.3" "pyarrow>=21.0.0" "psutil>=6.1.0" "memory-profiler>=0.61.0"
python -c "import pandas; print(f'✅ pandas {pandas.__version__} verified')"
python -c "import numpy; print(f'✅ numpy {numpy.__version__} verified')"
python -c "import pyarrow; print(f'✅ pyarrow {pyarrow.__version__} verified')"
python -c "import psutil; print(f'✅ psutil {psutil.__version__} verified')"
python -c "import memory_profiler; print(f'✅ memory-profiler verified')"

# Create Spark configuration directory
echo "⚙️  Configuring Spark environment..."
mkdir -p /opt/spark/conf

# Create Spark configuration file optimized for Codespaces
cat > /opt/spark/conf/spark-defaults.conf << 'EOF'
# DSAS 2025 - Spark Configuration for GitHub Codespaces

# Enable Adaptive Query Execution (AQE) for better performance
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.sql.adaptive.localShuffleReader.enabled=true

# Memory Configuration (optimized for 4-core, 16GB Codespace)
spark.driver.memory=6g
spark.driver.maxResultSize=2g
spark.executor.memory=4g
spark.executor.cores=4
spark.driver.cores=4

# Network and Gateway Configuration
spark.driver.host=127.0.0.1
spark.driver.bindAddress=127.0.0.1
spark.blockManager.port=0
spark.driver.port=0

# Enable Arrow for pandas integration
spark.sql.execution.arrow.pyspark.enabled=true
spark.sql.execution.arrow.pyspark.fallback.enabled=true

# UI Configuration
spark.ui.port=4040
spark.ui.enabled=true
spark.ui.host=0.0.0.0
spark.ui.showConsoleProgress=false

# Advanced Logging Configuration - Suppress execution plan spam
spark.sql.adaptive.logLevel=ERROR
spark.sql.execution.arrow.maxRecordsPerBatch=1000
spark.sql.adaptive.advisoryPartitionSizeInBytes=64MB

# Serialization (for better performance)
spark.serializer=org.apache.spark.serializer.KryoSerializer

# Local mode configuration
spark.master=local[*]
spark.sql.warehouse.dir=/tmp/spark-warehouse

# Ensure no deprecated configurations are used
spark.sql.adaptive.skewJoin.enabled=false

# Note: Advanced logging configuration is set above
EOF

# Create log4j configuration to reduce verbose logging
cat > /opt/spark/conf/log4j2.properties << 'EOF'
# DSAS 2025 - Logging Configuration

# Root logger option
rootLogger.level=WARN
rootLogger.appenderRef.console.ref=console

# Console appender configuration
appender.console.type=Console
appender.console.name=console
appender.console.target=SYSTEM_ERR
appender.console.layout.type=PatternLayout
appender.console.layout.pattern=%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n

# Reduce Spark logging verbosity
logger.spark.name=org.apache.spark
logger.spark.level=WARN

logger.hadoop.name=org.apache.hadoop
logger.hadoop.level=WARN

logger.hive.name=org.apache.hadoop.hive
logger.hive.level=WARN

# Suppress AdaptiveSparkPlanExec execution plan spam (DSAS-2025 Fix)
logger.adaptive.name=org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
logger.adaptive.level=ERROR
logger.adaptive.additivity=false

# Suppress other verbose execution logging
logger.catalyst.name=org.apache.spark.sql.catalyst
logger.catalyst.level=WARN
logger.catalyst.additivity=false

# Suppress SparkConf deprecation warnings (DSAS-2025 Fix)
logger.sparkconf.name=org.apache.spark.SparkConf
logger.sparkconf.level=ERROR
logger.sparkconf.additivity=false
EOF

# Set up environment variables in .bashrc
echo "🌐 Setting up environment variables..."
cat >> ~/.bashrc << 'EOF'

# DSAS 2025 - Apache Spark Environment
export SPARK_HOME=/opt/spark
export JAVA_HOME=/usr/local/sdkman/candidates/java/current
export PATH=$JAVA_HOME/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
export PYSPARK_PYTHON=/usr/local/bin/python3
export PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

# Spark optimization for Codespace
export SPARK_OPTS="--driver-memory=2g --driver-cores=2 --executor-memory=1g --executor-cores=2"
export SPARK_LOCAL_IP=127.0.0.1
export SPARK_DRIVER_HOST=127.0.0.1

# Helpful aliases
alias spark-shell='$SPARK_HOME/bin/spark-shell'
alias pyspark='$SPARK_HOME/bin/pyspark'
alias spark-sql='$SPARK_HOME/bin/spark-sql'
alias jupyter-lab='jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --ServerApp.token="" --ServerApp.password=""'
EOF

# Source the environment variables
source ~/.bashrc

# Create Jupyter configuration
echo "📓 Configuring Jupyter Lab..."
mkdir -p ~/.jupyter

cat > ~/.jupyter/jupyter_lab_config.py << 'EOF'
# DSAS 2025 - Jupyter Lab Configuration

c = get_config()  #noqa

# Server configuration
c.ServerApp.ip = '0.0.0.0'
c.ServerApp.port = 8888
c.ServerApp.open_browser = False
c.ServerApp.allow_root = True
c.ServerApp.token = ''
c.ServerApp.password = ''

# Enable extensions
c.ServerApp.jpserver_extensions = {
    'jupyterlab_git': True,
}

# File manager configuration
c.ContentsManager.allow_hidden = True

# Kernel configuration
c.MappingKernelManager.default_kernel_name = 'python3'
EOF

# Test Spark installation
echo "🧪 Testing Spark installation..."
export SPARK_HOME=/opt/spark
export PATH=$SPARK_HOME/bin:$PATH
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

# Quick Spark test
python3 -c "
import sys
sys.path.insert(0, '/opt/spark/python')
sys.path.insert(0, '/opt/spark/python/lib/py4j-0.10.9.7-src.zip')

try:
    from pyspark.sql import SparkSession
    print('✓ PySpark import successful')

    spark = SparkSession.builder.appName('CodespaceTest').master('local[1]').getOrCreate()
    print(f'✓ Spark Session created: {spark.version}')

    # Test basic functionality
    df = spark.range(10)
    count = df.count()
    print(f'✓ Basic Spark operation successful: {count} records')

    spark.stop()
    print('✓ Spark test completed successfully')

except Exception as e:
    print(f'❌ Spark test failed: {e}')
    sys.exit(1)
"

# Create student workspace directory
echo "📁 Creating student workspace..."
mkdir -p "/workspaces/dscc202-402-spring2026/Course Content/Module 1 - Foundation/Lab Notebooks/student-work"

echo ""
echo "✅ DSAS 2025 Spark Environment Setup Complete!"
echo "================================================="
echo ""
echo "🎯 Environment Details:"
echo "   • Apache Spark: ${SPARK_VERSION}"
echo "   • Python: $(python3 --version)"
echo "   • Jupyter Lab: Ready"
echo "   • Spark UI: http://localhost:4040"
echo ""
echo "📚 Next: Run 'bash .devcontainer/startup.sh' to start Jupyter Lab"
echo ""