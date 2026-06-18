# 🏎️ Formula 1 Data Lakehouse – Medallion Architecture Project

## 📌 Overview

This project implements a scalable **data lakehouse architecture** using the **Medallion Architecture (Bronze → Silver → Gold)** to analyze historical racing data from the **Formula 1**.

The solution is built on **Databricks** and uses **Azure Data Lake Storage (ADLS) external locations** for cloud-based data storage and processing.

This project processes full historical datasets (1950–2025) using a **full-load batch ingestion strategy (no incremental processing)**.

---

# ☁️ Architecture Overview

## 🔄 Medallion Flow

```text id="flow_full_load"
📂 ADLS Raw Data (Full Load)
        ↓
🥉 Bronze Layer (Raw Full Load Ingestion)
        ↓
🥈 Silver Layer (Full Data Transformation)
        ↓
🥇 Gold Layer (Star Schema Model)
        ↓
📊 BI Dashboards / ML Models
```

---

# ⚙️ Data Load Strategy

## 📌 Full Load Approach (Important Design Decision)

This project uses a **FULL LOAD strategy**, meaning:

* Entire dataset is reloaded during each pipeline run
* No CDC (Change Data Capture)
* No incremental watermarking
* No delta-based append logic

---

## 📊 Implications

✔ Simplifies pipeline design
✔ Ensures complete data consistency
✔ Suitable for historical analytics (1950–2025 dataset)
❌ Higher compute cost compared to incremental
❌ Not optimized for real-time ingestion

---

# 📊 Data Sources

* `constructors.json`
* `drivers.json`
* `races.csv`
* `circuits.csv`
* `results/1950–2025/*.json`
* `sprints/2021–2025/*.json`

All data is reprocessed completely during each run.

---

# 🥉 Bronze Layer (Raw Full Load)

## 📌 Purpose

Store full dataset snapshots exactly as received from ADLS.

## 📦 Tables

* `bronze_drivers`
* `bronze_constructors`
* `bronze_races`
* `bronze_circuits`
* `bronze_results`
* `bronze_sprints`

---

## 🧱 Characteristics

* Full dataset reload on every pipeline run
* No deduplication logic at ingestion stage
* Schema preserved as-is
* Metadata added:

  * ingestion_timestamp
  * source_file_path
  * load_batch_id

---

## 📁 Storage

```text id="bronze_path"
abfss://f1-data/.../bronze/
```

---

# 🥈 Silver Layer (Full Transformation Layer)

## 📌 Purpose

Clean and transform **entire dataset in every run**.

---

## 🔧 Transformations

* Data type casting
* Deduplication across full dataset
* JSON flattening
* Column standardization
* Full joins across drivers, constructors, races

---

## 📊 Silver Tables

* `silver_drivers`
* `silver_constructors`
* `silver_circuits`
* `silver_races`
* `silver_race_results`
* `silver_sprint_results`

---

## 📁 Storage

```text id="silver_path"
abfss://f1-data/.../silver/
```

---

# 🥇 Gold Layer (Star Schema Analytics Model)

## 📌 Purpose

Build a **fully refreshed dimensional model** on every run.

---

# ⭐ Dimension Tables

## 👨‍✈️ dim_drivers

**PK:** `driver_id`

---

## 🏎️ dim_constructors

**PK:** `constructor_id`

---

## 🏁 dim_races

**Composite PK:** `(season, round)`

---

# 📊 Fact Table

## 🏎️ fact_session_results

### 📌 Grain

One record per:

> season + round + session + type + driver + constructor

---

### 🔑 Composite Key

```text id="fact_key_full_load"
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

## 📁 Storage

```text id="gold_path"
abfss://f1-data/.../gold/
```

---

# 🔗 Star Schema Design

```text id="star_schema_full_load"
                 dim_drivers
                      │
                      │
dim_races ─── fact_session_results ─── dim_constructors
 (season,round)
```

---

# 📈 Business Use Cases

* Season-wise driver performance comparison
* Constructor championship analysis
* Race strategy evaluation
* Sprint vs race performance comparison
* Grid position impact on race outcome

---

# ⚙️ Tech Stack

* PySpark / Spark SQL
* Databricks Notebooks
* Delta Lake
* Azure Data Lake Storage Gen2 (External Location)
* Unity Catalog

---

# 🔐 Data Architecture Highlights

✔ Full-load batch processing (no incremental logic)
✔ Medallion architecture design pattern
✔ Star schema modeling in Gold layer
✔ Cloud-native storage using ADLS external locations
✔ Large-scale historical dataset (1950–2025)
✔ Multi-session Formula 1 analytics

---

# 🚀 Future Enhancements

* Convert to incremental pipeline (Delta MERGE strategy)
* Streaming race telemetry ingestion
* ML model for podium prediction
* Power BI dashboard layer
* Data quality validation framework

---

# 🧠 Final Summary

This project demonstrates a **complete end-to-end data engineering solution** featuring:

* Full-load batch ingestion strategy
* Medallion architecture design
* Azure Data Lake Storage (external locations)
* Star schema dimensional modeling
* Large-scale Formula 1 analytics pipeline
* Production-style Databricks implementation
