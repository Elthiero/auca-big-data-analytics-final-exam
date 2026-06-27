# Distributed Multi-Model Analytics for E-Commerce Data

This repository contains the final project for the AUCA Big Data Analytics course. It implements an end‑to‑end analytics pipeline for a synthetic e‑commerce dataset using **MongoDB** (document store), **HBase** (wide‑column store), and **Apache Spark** (distributed processing).

## System Architecture Overview  

The system is deployed using Docker Compose, with three core components orchestrated as follows:

![Architecture Diagram](screenshots/system_design.jpg)  
*Figure 1: End‑to‑end data pipeline – data generation → ingestion into MongoDB/HBase → Spark processing → visualisation*

**Data Generation** (`generator.py`) produces JSON files for users, categories, products, sessions (split into 20 chunks) and transactions.  
**Data Ingestion**  

- MongoDB: `1_load_to_mongodb.py` transforms and loads entities with embedded summaries and materialised views.  
- HBase: `1_load_to_hbase.py` creates two tables (`user_sessions`, `product_metrics`) using Thrift and loads session data with sparse activity columns.  
**Processing**  
- Apache Spark (`2_spark_analysis.py`) reads raw JSON, cleans data, computes product affinities, and executes SQL analytics.  
- Integration scripts (`3_integrated_analytics.py`, `3_integrated_visual.py`) combine MongoDB and HBase data to produce funnel metrics and user‑journey reconstructions.  
**Visualisation**  
- `4_visualizations.py` generates static charts using Matplotlib and Seaborn.

## Table of Contents

- [Distributed Multi-Model Analytics for E-Commerce Data](#distributed-multi-model-analytics-for-e-commerce-data)
  - [System Architecture Overview](#system-architecture-overview)
  - [Table of Contents](#table-of-contents)
  - [Overview](#overview)
  - [Repository Structure](#repository-structure)
  - [Prerequisites](#prerequisites)
  - [Setup \& Installation](#setup--installation)
    - [1. Clone the repository](#1-clone-the-repository)
    - [2. Create a virtual environment (recommended)](#2-create-a-virtual-environment-recommended)
    - [3. Install Python dependencies](#3-install-python-dependencies)
    - [4. Configure environment variables](#4-configure-environment-variables)
  - [Step-by-Step Execution](#step-by-step-execution)
    - [1. Start the Containers](#1-start-the-containers)
    - [2. Generate the Dataset](#2-generate-the-dataset)
    - [3. Load Data into MongoDB](#3-load-data-into-mongodb)
    - [4. Load Data into HBase](#4-load-data-into-hbase)
    - [5. Run Spark Analytics](#5-run-spark-analytics)
    - [6. Run Integrated Analytics](#6-run-integrated-analytics)
      - [6.1 Conversion Funnel (Macro)](#61-conversion-funnel-macro)
      - [6.2 User Journey Reconstruction (Micro)](#62-user-journey-reconstruction-micro)
    - [7. Generate Visualizations](#7-generate-visualizations)
  - [Outputs](#outputs)
  - [Troubleshooting](#troubleshooting)
  - [Environment Variables](#environment-variables)
  - [License](#license)

---

## Overview

The project demonstrates how to:

- Design query‑driven schemas in MongoDB (embedding categories, lifetime summaries, and transaction line items).
- Leverage HBase’s sparse wide‑column model for time‑series clickstream data (reversed‑timestamp row keys, dynamic column qualifiers).
- Use PySpark for data cleaning, batch processing (product co‑occurrence matrix), and SQL‑based cohort analysis.
- Integrate both databases to compute a cross‑system conversion funnel and reconstruct a user’s chronological journey.
- Generate actionable business visualisations (funnel, geographic revenue, product affinity).

**Technology Stack**:

- **MongoDB** (6.0) – document database
- **HBase** (2.1) – wide‑column store (Thrift API on port 9090)
- **Apache Spark** (3.x) – distributed processing (local mode)
- **Python** (3.8+) – ingestion, analysis, and visualisation
- **Docker** & **Docker Compose** – container orchestration

---

## Repository Structure

```text
.
├── docker-compose.yml          # MongoDB & HBase container definitions
├── config.py                   # Central configuration (loads .env)
├── logger.py                   # Standardised logging utility
├── generator.py                # Synthetic dataset generator (2M sessions, 500k transactions)
├── 1_load_to_mongodb.py        # MongoDB ingestion + aggregation pipelines
├── 1_load_to_hbase.py          # HBase table creation & session loading
├── 2_spark_analysis.py         # PySpark cleaning, affinity matrix, Spark SQL
├── 3_integrated_visual.py      # Integrated funnel analysis (HBase + MongoDB)
├── 3_integrated_analytics.py   # User journey reconstruction (cross‑system)
├── 4_visualizations.py         # Static chart generation (state revenue & affinity)
├── requirements.txt            # Python dependencies
├── .env.example                # Template for environment variables
├── data/                       # Generated JSON files (created by generator.py)
└── visualizations/             # Output charts (created by scripts)
```

---

## Prerequisites

- **Docker** and **Docker Compose** (≥ 2.0)
- **Python 3.8+** with `pip`
- At least **8 GB RAM** (Spark and HBase together are memory‑hungry)
- ~ **2 GB free disk space** for the generated dataset

---

## Setup & Installation

### 1. Clone the repository

```bash
git clone https://github.com/Elthiero/auca-big-data-analytics-final-exam.git
cd auca-big-data-analytics-final-exam
```

### 2. Create a virtual environment (recommended)

```bash
python -m venv venv
source venv/bin/activate      # Linux/macOS
# or
venv\Scripts\activate         # Windows
```

### 3. Install Python dependencies

```bash
pip install -r requirements.txt
```

**Contents of `requirements.txt`**:

```text
faker>=18.0.0
numpy>=1.24.0
pymongo>=4.5.0
happybase>=1.2.0
pyspark>=3.4.0
matplotlib>=3.7.0
seaborn>=0.12.0
python-dotenv>=1.0.0
```

### 4. Configure environment variables

Copy the example environment file and adjust if needed:

```bash
cp .env.example .env
```

Default values already work for the local Docker setup. See [Environment Variables](#environment-variables) for details.

---

## Step-by-Step Execution

> **Important**: Run all commands from the repository root directory.

### 1. Start the Containers

Launch MongoDB and HBase in the background:

```bash
docker-compose up -d
```

Wait **30–60 seconds** for HBase to fully initialise its Thrift server. You can check the logs:

```bash
docker-compose logs -f hbase
```

When you see `ThriftServer started`, HBase is ready.

### 2. Generate the Dataset

The generator produces:

- `users.json` (10 000 users)
- `categories.json` (25 categories)
- `products.json` (5 000 products)
- `transactions.json` (500 000 transactions)
- `sessions_0.json` … `sessions_19.json` (2 000 000 sessions, split into 20 chunks)

```bash
python generator.py
```

**Estimated time**: ~5–10 minutes depending on your machine. The script logs progress every 50k iterations.

### 3. Load Data into MongoDB

This script transforms the raw JSON into the final document model:

- Embeds category hierarchies into products.
- Adds `lifetime_summary` and `segmentation_tags` to users.
- Denormalises product names/categories into transaction items.
- Creates the `daily_sales_by_category` materialised view.

```bash
python 1_load_to_mongodb.py
```

**Expected output**: Log messages showing counts of loaded documents and index creation.

### 4. Load Data into HBase

This script creates two HBase tables:

- `user_sessions`: row key `user_id|reversed_timestamp`, with column families `session`, `geo`, `device`, `activity`.
- `product_metrics`: row key `product_id|YYYYMMDD`, with column families `daily`, `aggregates`.

It then reads all `sessions_*.json` files and inserts them in batches.

```bash
python 1_load_to_hbase.py
```

**Note**: HBase Thrift might time out if the container hasn’t fully started. If you see `TTransportException`, wait a minute and re‑run.

**Expected output**: A log line showing `Total sessions loaded: 2000000`.

### 5. Run Spark Analytics

This script performs three main tasks:

- **Data Cleaning**: explicit schemas, timestamp casting, null handling.
- **Batch Processing**: computes a product co‑occurrence matrix (“users who viewed X also viewed Y”).
- **Spark SQL**: joins users and transactions to find top‑revenue states.

```bash
python 2_spark_analysis.py
```

**Expected output**:

- Top 5 product affinity pairs (printed to console).
- Top 10 states by revenue (printed to console).

### 6. Run Integrated Analytics

#### 6.1 Conversion Funnel (Macro)

This script scans HBase for total sessions and cart‑add events, then queries MongoDB for completed purchases, and saves a funnel chart.

```bash
python 3_integrated_visual.py
```

This scans **2 million rows** in HBase via the Thrift client and may take **3–5 minutes**. The output chart is saved as `visualizations/conversion_funnel.png`.

#### 6.2 User Journey Reconstruction (Micro)

This script picks a random completed transaction from MongoDB, finds its corresponding session in HBase, and reconstructs the chronological page‑view sequence.

```bash
python 3_integrated_analytics.py
```

**Expected output**: A nicely formatted console print showing each step (page type, duration, product ID) leading to the purchase.

### 7. Generate Visualizations

Finally, generate the remaining two static charts:

- `state_revenue.png` – Top 10 states by revenue.
- `product_affinity.png` – Top 5 co‑viewed product pairs.

```bash
python 4_visualizations.py
```

All charts are saved in the `visualizations/` directory.

---

## Outputs

After running all steps, you should have:

| File | Description |
|------|-------------|
| `visualizations/conversion_funnel.png` | Funnel: total sessions → carts → purchases |
| `visualizations/state_revenue.png` | Bar chart of revenue per state |
| `visualizations/product_affinity.png` | Horizontal bar chart of product pairs |
| Console logs | Progress, warnings, and query results from each script |

---

## Troubleshooting

| Issue | Likely Cause | Solution |
|-------|--------------|----------|
| `TTransportException` / `TSocket read 0 bytes` | HBase Thrift server not ready or using REST port. | Check `docker-compose logs hbase`. Ensure `HBASE_TYPE=thrift` is set. Restart with `docker-compose restart hbase`. |
| `OutOfMemoryError` in Spark | Spark tries to infer schema on large JSON files. | The script uses explicit schemas to prevent this. If it still occurs, increase driver memory in `config.py` or set `SPARK_MEM` environment variable. |
| `pymongo.errors.ServerSelectionTimeoutError` | MongoDB container not running or port 27017 is occupied. | Run `docker-compose ps` to verify both containers are `Up`. |
| HBase table creation fails with `TypeError: endswith first arg must be bytes` | Byte‑string passed in column‑family definition. | The script uses plain strings for family names – this error should not occur. If it does, remove `b''` prefixes from family dictionaries. |
| `generator.py` runs very slowly | Generating 2M sessions is CPU‑intensive. | It is normal. Let it run; progress is logged every 50k iterations. |
| Scripts cannot find `data/` folder | Generator not run yet, or running from wrong directory. | Ensure you are in the repository root and have run `python generator.py` first. |

---

## Environment Variables

The `.env` file (optional) overrides defaults in `config.py`:

| Variable | Default | Description |
|----------|---------|-------------|
| `LOG_LEVEL` | `INFO` | Logging verbosity (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |
| `MONGO_URI` | `mongodb://localhost:27017/` | MongoDB connection string |
| `MONGO_DB_NAME` | `ecommerce_analytics` | Database name used by all scripts |
| `HBASE_HOST` | `localhost` | HBase Thrift server host |
| `HBASE_PORT` | `9090` | HBase Thrift port |
| `SPARK_APP_NAME` | `ECommerceAnalytics` | Application name shown in Spark UI |
| `SPARK_MASTER` | `local[*]` | Spark master URL (use `local[*]` for all cores) |

---

## License

This project was developed for educational purposes as part of the AUCA Big Data Analytics course.  
All code is provided “as is” for demonstration and learning.
