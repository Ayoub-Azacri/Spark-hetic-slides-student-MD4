# ONISR Accidents Spark Pipeline

Welcome to the **ONISR Accidents Spark Pipeline** repository! 🚗💥
This project builds an end-to-end data engineering pipeline using Apache Spark (PySpark) to ingest, clean, optimize, and analyze a large-scale dataset of road accidents in France (ONISR 2023). It goes from raw CSV ingestion to structured Parquet storage, analytics aggregates, and a live Structured Streaming simulator.

---

## 🏗️ Data Architecture

```
data/datasets/onisr-2023/ (CSV)  →  pipeline.py (ETL)  →  data/output/clean/ (Silver Parquet)
                                                                     │
                                                                     ▼
                                                        data/output/analyses/ (Gold CSV)
```

The pipeline follows a multi-tier lakehouse strategy:
1. **Bronze (Raw)**: Ingestion of 4 relational CSV tables (`caracteristiques`, `lieux`, `vehicules`, `usagers`) with strict schemas (`StructType`).
2. **Silver (Cleaned)**: Cleaning (deduplication, coordinates conversion, timestamp building) and writing to Parquet. The `caracteristiques` table is partitioned by department (`dep`) to enable Partition Pruning.
3. **Gold (Aggregated)**: High-performance business queries using joins, caching, broadcast variables, and window functions.

---

## 🚀 Project Features

### F1 — Ingestion & Strict Typage
Explicit schema parsing (`StructType`) on 4 relational tables, casting numbers, dates, and handling CSV-specific character encodings.

### F2 — Data Cleaning & Parquet Writing
Deduplication on primary keys, GPS coordinates normalization (comma-to-dot mapping), standardization of timestamps, and dropping of out-of-bounds/corrupted rows.

### F3 — Business Analytics
3 distinct business analyses exported to CSV:
*   **Analyse 1 (Aggregation)**: Accident gravity breakdown by weather conditions (`atm`).
*   **Analyse 2 (Join)**: Fatality/Hospitalization rate by vehicle category (`catv`).
*   **Analyse 3 (Window Function)**: Monthly ranking of the top 3 most dangerous departments in France.

### F4 — Caching & Broadcast Joins
Optimization of hot paths via `.cache()` and execution of `F.broadcast()` on small lookup tables, reducing query latency by over 50% and eliminating shuffle overhead.

### F5 — Deep Spark UI Analysis
Tuning pipeline execution by inspecting Spark UI DAGs, task timelines, and physical plans (`.explain()`) to resolve shuffle bottlenecks.

### F6 — Advanced Explorations
4 detailed engineering benchmarks documented in the report:
*   **Format Benchmark**: CSV vs Parquet vs JSON (write speeds, size on disk, query latency).
*   **Caching Impact**: Performance gains of caching reused DataFrames.
*   **Predicate Pushdown**: FileScan filtering directly at the Parquet metadata layer.
*   **Shuffle Tuning**: Redistribution network costs (Repartition vs Coalesce).

### F7 — Structured Streaming (Bonus)
A real-time Structured Streaming pipeline (`bonus_streaming.py`) listening to a streaming directory. A background daemon thread simulates continuous ingestion by dropping batches of 15,000 rows every 6 seconds, producing dynamic department rankings in the console.

#### 🔄 Streaming Flow Architecture:
```
[ caract-2023.csv (Données Brutes) ]
                 │
                 ▼  (Simulateur : Découpe en lots de 15 000 lignes)
[ data/streaming_input/ ]  ◄── (Dépose un fichier toutes les 6 secondes)
                 │
                 ▼  (Spark .readStream)
[ Spark Engine (Traitement du flux) ]
                 │
                 ▼  (Agrégation en direct : count par département)
[ Console Output (.writeStream.format("console")) ] (Mise à jour en direct)
```

---

## ⚡ Quick Start

```bash
# 1. Clone
git clone https://github.com/Ayoub-Azacri/Spark-hetic-slides-student-MD4.git
cd Spark-hetic-slides-student-MD4

# 2. Setup environment and install PySpark
python3 -m venv .venv
source .venv/bin/activate
pip install -r starter-code/requirements.txt

# 3. Download ONISR 2023 dataset (Ensure Java 17+ is installed)
bash data/download_onisr.sh

# 4. Run main ETL and Analytics pipeline
.venv/bin/python starter-code/pipeline.py

# 5. Run Live Structured Streaming Bonus Simulation
.venv/bin/python starter-code/bonus_streaming.py
# (Press Ctrl+C to terminate)
```

---

## 📊 Key Numbers

| Metric | Value |
|--------|-------|
| Raw Ingested Rows | 345,056 rows |
| Dropped Corrupted Rows | 16,156 rows |
| Silver Parquet Files | 240 partition files |
| Caching Speedup | 3.3x faster (Run 2/3) |
| Broadcast Join Speedup | ~1.8 seconds saved |
| Streaming Simulation Rate | 15,000 rows / 6 seconds |

---

## 👥 Team

| Member | Role | Contributions |
|--------|------|---------------|
| **Ayoub AZACRI** | Data Engineer | Ingestion Silver · Format Benchmark · Structured Streaming Ingestion |
| **Omar HAKIK** | Data Quality | Data Cleaning · Predicate Pushdown Exploration · Aggregations |
| **Youssef EL HAJJI** | Lead Tech | Caching Optimizations · Simulator & Console Streaming Output |
| **Youssef DEKHAIL** | Data Analyst | Window Functions · Dashboard & Documentation |

---

## 📁 Repository Structure

```
Spark-hetic-slides-student/
├── data/
│   └── datasets/onisr-2023/    # raw datasets (gitignored)
│   └── output/
│       ├── clean/              # Silver Parquet partitioned by dep
│       └── analyses/           # Gold CSV results
├── projects/
│   ├── rapport-modele.md       # Project Synthesis Report
│   ├── README.md               # This project README
│   └── screenshots/            # Spark UI & Streaming screenshots
├── starter-code/
│   ├── pipeline.py             # Main ETL script
│   ├── bonus_streaming.py       # Live Streaming script
│   └── requirements.txt
```

---

## 🌟 About Us

We are a group of Data Engineering students passionate about building highly scalable distributed pipelines and exploring Apache Spark execution optimization.
