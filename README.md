# DSCC-202-402 Spring 2026

Course materials for Data Science at Scale - Apache Spark Fundamentals

## Overview

This course teaches distributed data processing with Apache Spark. Students will learn RDD fundamentals, DataFrames, Spark SQL, and performance optimization techniques. The course uses **two complementary learning environments** to provide comprehensive Spark experience:

- **GitHub Codespaces** (Supplementary) - For RDD operations and Spark UI optimization
- **Databricks Free Edition** (Primary) - For main lab and project work

---

## Learning Environments

### GitHub Codespaces - Supplementary Environment

Use GitHub Codespaces for **RDD fundamentals** and **Spark UI exploration** where Databricks Free Edition has limitations.

#### Why Use Codespaces?

Databricks Free Edition has restrictions that limit certain learning objectives:
- **Limited RDD API access** - Some RDD operations are restricted
- **Limited Spark UI access** - Cannot view detailed DAGs, stages, and performance metrics needed for optimization learning

GitHub Codespaces provides:
- **Full RDD API access** - No restrictions on RDD operations
- **Complete Spark UI** - Full access to DAG visualization, stage details, task metrics, and performance profiling (port 4040)
- **Consistent environment** - Same setup for all students, cloud-based, no local installation needed
- **Apache Spark 3.5.0** - Latest stable version with full configuration control

#### What's Included

- **Apache Spark 3.5.0** with Jupyter Lab
- **7 Notebooks** covering:
  - 1: RDD Fundamentals
  - 2: Transformations vs Actions
  - 3: Lazy Evaluation
  - 4: DataFrame API Introduction
  - 5: Spark SQL Basics
  - 6: DataFrame Operations
  - 7: User-Defined Functions
- **7 Curated Datasets** - Customer data, transactions, IoT sensors, social media, product catalog
- **Pre-configured environment** - Optimized for learning with proper resource allocation

#### Getting Started with Codespaces

1. **Learn about Codespaces**: [GitHub Codespaces Quickstart Guide](https://docs.github.com/en/codespaces/quickstart)

2. **Launch your Codespace**:
   - Go to this repository on GitHub
   - Click the green **Code** button
   - Select **Codespaces** tab
   - Click **Create codespace on master**

3. **Wait for setup** (3-5 minutes):
   - Apache Spark will be automatically installed
   - Jupyter Lab will start automatically
   - All labs and datasets will be ready

4. **Start learning**:
   - Jupyter Lab opens at `http://localhost:8888`
   - Navigate to `examples/spark-core-notebooks/`
   - Start with **Lab 1 - RDD Fundamentals - Solutions.ipynb**

#### Accessing the Spark UI

The Spark UI is essential for understanding performance and optimization:

1. When running Spark code in a notebook, the UI becomes available at `http://localhost:4040`
2. In GitHub Codespaces:
   - Look for the **PORTS** tab in the bottom panel
   - Find port **4040** and click the globe icon to open
3. Explore:
   - **Jobs** - See triggered actions and their execution
   - **Stages** - View task distribution and shuffle operations
   - **Storage** - Monitor cached RDDs and DataFrames
   - **Environment** - Check Spark configuration
   - **Executors** - Observe resource utilization

#### Available Notebooks and Datasets

** Notebooks** (located in `examples/spark-core-notebooks/`):
- All labs include solution code with explanations
- Each lab has hands-on exercises with validation
- Real-world business scenarios and datasets

**Datasets** (located in `examples/Datasets/`):
- `customers.csv` - Customer demographics and profiles
- `customer_transactions.csv` - Transaction records with categories
- `product_catalog.csv` - Product inventory data
- `social_media_users.csv` & `social_media_posts.csv` - Social media analytics
- `iot_sensor_readings.csv` & `iot_sensor_readings.parquet` - IoT sensor data in multiple formats

#### Technical Details

For detailed technical information about the Codespaces environment, see [.devcontainer/README.md](.devcontainer/README.md)

---

### Databricks Free Edition - Primary Environment

**Databricks Free Edition is the primary platform** for most course work, including labs and projects.

#### Access Databricks Workspace

[Establish your Databricks Free Account](https://www.databricks.com/learn/free-edition)

#### Getting Started with Databricks

Here is helpful information about importing archives into the Databricks Environment:
[Getting Started with Databricks](https://docs.databricks.com/aws/en/getting-started/)

Import the DBC archive from the Learning Spark v2 GitHub repository into your account. (This is the code that goes along with the textbook):
[DBC Archive](https://github.com/databricks/LearningSparkV2/blob/master/notebooks/LearningSparkv2.dbc)

#### When to Use Each Environment

| Task | Environment | Reason |
|------|-------------|--------|
| RDD operations and fundamentals | **GitHub Codespaces** | Full RDD API access |
| Spark UI exploration and optimization | **GitHub Codespaces** | Complete DAG and performance metrics |
| Course labs and assignments | **Databricks** | Primary platform |
| Course projects | **Databricks** | Primary platform |
| Learning Spark v2 textbook examples | **Databricks** | Provided DBC archive |

**Note:** For topics involving RDD operations or Spark UI analysis, use the supplementary GitHub Codespaces environment. For all other course work, use Databricks Free Edition.

---

## GitHub Setup

### Establishing a GitHub Account

If you don't have a GitHub account already:
[Sign up for a new GitHub account](https://docs.github.com/en/github/getting-started-with-github/signing-up-for-a-new-github-account)

### Fork the Class Repository

Fork this repository into your own GitHub account. This will create a copy of the course repo for you to work with.

**To fork:**
1. Go to https://github.com/lpalum/dscc202-402-spring2026
2. Click the **Fork** button (top right) while logged into your GitHub account
3. This creates: `https://github.com/YOUR-USERNAME/dscc202-402-spring2026`

### Connect Your Forked Repository to Databricks

Instead of cloning to your local machine, you'll import your forked repository directly into your Databricks workspace using Git Folders.

#### Step 1: Link Your GitHub Account to Databricks

1. In your Databricks workspace, click your **username** (top right) → **Settings**
2. Go to **Linked accounts** tab → **Add Git credential**
3. Select **GitHub** → **Link Git account**
4. On the GitHub authorization page, click **Authorize Databricks**
5. Install the Databricks GitHub App and select your repositories

**Note:** You only need to do this once.

#### Step 2: Clone Your Repository into Databricks

1. In the Databricks sidebar, click **Workspace**
2. Navigate to your user folder
3. Click **Create** → **Git folder**
4. Provide:
   - **Git repository URL**: `https://github.com/YOUR-USERNAME/dscc202-402-spring2026`
   - **Git provider**: GitHub
   - **Git folder name**: `dscc202-402-spring2026`
5. Click **Create Git folder**

#### Step 3: Verify Your Setup

1. Expand the Git folder in your workspace
2. Navigate to `labs/` (if available) and open a notebook
3. Verify the notebook opens and shows the Git branch indicator at the top

**Troubleshooting:**
- Verify your GitHub username is correct in the URL
- Ensure you completed Step 1 (linking GitHub account)
- Check that your forked repository is public or Databricks has access

#### Working with Git Folders

- **Make changes**: Edit notebooks directly in Databricks
- **Commit & push**: Use the Git menu (top right of notebook) to commit and push to GitHub
- **Pull updates**: Use the Git menu to pull changes from your fork

---

### Support and Resources

- **Codespaces Issues**: See [.devcontainer/README.md](.devcontainer/README.md) troubleshooting section
- **Databricks Issues**: Check [Databricks Getting Started Guide](https://docs.databricks.com/aws/en/getting-started/)
- **GitHub Issues**: Report repository issues at https://github.com/lpalum/dscc202-402-spring2026/issues

---

## Quick Reference

### GitHub Codespaces
- **Purpose**: RDD operations, Spark UI exploration
- **Access**: Code button → Codespaces → Create codespace
- **Jupyter Lab**: `http://localhost:8888`
- **Spark UI**: `http://localhost:4040` (check PORTS tab)
- **Labs**: `examples/spark-core-notebooks/`

### Databricks Free Edition
- **Purpose**: Primary platform for labs and projects
- **Access**: https://www.databricks.com/learn/free-edition
- **Learning Spark v2**: Import DBC from GitHub
- **Git Integration**: Link account → Create Git folder

### Your Forked Repository
- **URL**: `https://github.com/YOUR-USERNAME/dscc202-402-spring2026`
- **Connect to Databricks**: Git Folders feature
- **Connect to Codespaces**: Code button → Create codespace

---

Happy learning! 🎓
