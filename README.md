# 🏎️ Formula 1 Data Lakehouse – Medallion Architecture Project

## 📌 Overview

This project implements a scalable **data lakehouse architecture** using the **Medallion Architecture (Bronze → Silver → Gold)** to analyze historical racing data from the **Formula 1**.

The pipeline is built using **Databricks**, with data stored in **Azure Data Lake Storage (ADLS) Gen2 external locations** and processed using Spark and Delta Lake.

The system processes full historical datasets (1950–2025) using a **full-load batch ingestion strategy** and produces a **star-schema-based analytics warehouse** for BI and ML use cases.

---

# ☁️ Cloud Architecture

## 🧱 Storage Layer (ADLS Gen2)

All data is stored in **Azure Data Lake Storage Gen2** using Databricks external locations.

```text id="adls_structure"
abfss://f1-data@<storage-account>.dfs.core.windows.net/
│
├── bronze/
├── silver/
└── gold/
```

✔ Managed via Unity Catalog
✔ No DBFS dependency
✔ Secure access using service principal / managed identity

---

# ⚙️ Data Load Strategy

## 📌 FULL LOAD PIPELINE

This project uses a **full-load batch processing strategy**.

### Characteristics:

* Entire dataset is reprocessed on every run
* No incremental loads or CDC
* No watermarking
* Simplified orchestration logic

### Trade-offs:

✔ Data consistency
✔ Simplicity
❌ Higher compute cost
❌ Not real-time

---

# 🏗️ Medallion Architecture

## 🔄 Pipeline Flow

```text id="medallion_flow"
📂 ADLS Raw Data (Full Load)
        ↓
🥉 Bronze Layer (Raw ingestion)
        ↓
🥈 Silver Layer (Cleaned & Conformed)
        ↓
🥇 Gold Layer (Star Schema Model)
        ↓
📊 BI Dashboards / ML Models
```

---

# 📊 Data Sources

* `constructors.json`
* `drivers.json`
* `races.csv`
* `circuits.csv`
* `results/1950–2025/*.json`
* `sprints/2021–2025/*.json`

---

# 🥉 Bronze Layer (Raw Ingestion)

## 📌 Purpose

Store raw data exactly as received from ADLS.

## 📦 Tables

* `bronze_drivers`
* `bronze_constructors`
* `bronze_races`
* `bronze_circuits`
* `bronze_results`
* `bronze_sprints`

## 🧱 Characteristics

* Schema preserved as-is
* Append-only ingestion
* Full dataset reload each run
* Metadata added:

  * ingestion_timestamp
  * file_path
  * batch_id

---

# 🥈 Silver Layer (Cleaned & Conformed)

## 📌 Purpose

Transform raw data into analytics-ready structured datasets.

## 🔧 Transformations

* Data type casting
* Deduplication
* JSON flattening
* Column standardization
* Full dataset joins

---

## 📊 Silver Tables

* `silver_drivers`
* `silver_constructors`
* `silver_circuits`
* `silver_races`
* `silver_race_results`
* `silver_sprint_results`

---

# 🥇 Gold Layer (Star Schema Model)

## 📌 Purpose

The Gold layer is modeled as a **dimensional star schema** for analytics and BI.

---

# ⭐ Dimension Tables

## 👨‍✈️ dim_drivers

**PK:** `driver_id`

* driver_id
* driver_name
* nationality
* date_of_birth

---

## 🏎️ dim_constructors

**PK:** `constructor_id`

* constructor_id
* constructor_name
* nationality

---

## 🏁 dim_races

**Composite PK:** `(season, round)`

* season
* round
* race_name
* race_date
* circuit_id
* country

---

# 📊 Fact Table

## 🏎️ fact_session_results

### 📌 Grain

One record per:

> season + round + session + type + driver + constructor

---

### 🔑 Composite Key

```text id="fact_key"
(season, round, session, type, constructor_id, driver_id)
```

---

### 📦 Measures

* grid_position
* final_position
* points
* laps_completed
* status
* fastest_lap_time

---

# 🔗 Star Schema

```text id="star_schema"
                 dim_drivers
                      │
                      │
dim_races ─── fact_session_results ─── dim_constructors
 (season,round)
```

---

# ⚙️ Databricks Workflow Orchestration

This project uses **Databricks Workflows (Jobs API)** to orchestrate the full pipeline.

---

## 🔄 Pipeline Structure

### 🥉 Bronze Layer (Parallel ingestion)

* Circuits ingestion
* Drivers ingestion
* Constructors ingestion
* Races ingestion
* Results ingestion (1950–2025)
* Sprints ingestion (2021–2025)

---

### 🥈 Silver Layer (Transformations)

Each task depends on its corresponding bronze ingestion.

---

### 🥇 Gold Layer (Star Schema Build)

* `dim_races`
* `dim_drivers`
* `dim_constructors`
* nationality reference enrichment (MDM layer)

---

### 📊 Fact Layer

* `fact_session_results`

Combines:

* Race results
* Sprint results
* Driver performance
* Constructor performance

---

## 🔗 DAG Flow

```text id="dag"
Bronze → Silver → Gold Dimensions → Fact Table
```

---

## 📌 Key YAML Workflow Features

✔ Task-level dependencies
✔ Parallel ingestion execution
✔ Multi-stage DAG orchestration
✔ Environment configuration
✔ Retry and performance optimization
✔ Full pipeline automation

---

# 📈 Business Use Cases

* Driver championship analysis
* Constructor performance tracking
* Race strategy evaluation
* Sprint vs race comparison
* Grid position impact analysis

---

# ⚙️ Tech Stack

* PySpark / Spark SQL
* Databricks Workflows
* Delta Lake
* Azure Data Lake Storage Gen2 (External Locations)
* Unity Catalog

---

# 🔐 Data Governance

* External locations managed via Unity Catalog
* Secure cloud storage access
* No DBFS dependency
* Role-based access control (RBAC)

---

# 🔑 Key Design Principles

✔ Medallion architecture
✔ Star schema dimensional modeling
✔ Full-load batch processing
✔ Cloud-native storage (ADLS Gen2)
✔ Workflow-based orchestration
✔ Multi-session Formula 1 analytics

---

# 🚀 Future Enhancements

* Incremental processing (Delta MERGE)
* Streaming telemetry ingestion (Kafka/Event Hub)
* ML model for race outcome prediction
* Power BI / Tableau dashboards
* Data quality framework (Deequ / Great Expectations)

---

# 🧠 Final Summary

This project demonstrates a **production-grade data engineering solution** featuring:

* Full-load Medallion architecture
* Azure Data Lake Storage external locations
* Star schema dimensional modeling
* Databricks Workflow orchestration (DAG)
* Large-scale Formula 1 analytics pipeline
* BI + ML-ready data warehouse design

🚀 Write **FAANG-level CV bullet points**
🚀 Or prepare a **system design interview explanation script (walkthrough)**
