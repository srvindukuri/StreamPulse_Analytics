# StreamPulse Analytics ETL Project

This project implements an end-to-end ETL and streaming data pipeline for real-time music streaming analytics. The objective is to simulate ingestion, processing, transformation, and aggregation of streaming events such as user plays, skips, likes, and session details. The pipeline leverages Apache Kafka, Apache Spark Structured Streaming, Apache Airflow, and PostgreSQL to deliver a production-grade data engineering workflow.

---

## 🧱 Project Architecture

```
StreamPulse_Analytics/
├── dags/                      # Airflow DAGs to orchestrate batch ETL
├── spark_jobs/                # PySpark streaming and batch transformation scripts
├── data/                      # Source seed data (user, song, artist metadata)
├── warehouse/                 # Output data: fact and dimension tables
├── tmp/                       # Staging / checkpoint directories
├── docker-compose.yaml        # Multi-container setup: Kafka, Spark, Airflow, Postgres
├── Dockerfile                 # Custom Spark image
├── requirements.txt           # Python dependencies
└── README.md                  # Project documentation
```

---

## ⚙️ Technologies Used

| Technology                                    | Purpose                              |
| --------------------------------------------- | ------------------------------------ |
| Apache Airflow                                | Workflow orchestration for batch ETL |
| Apache Spark (Structured Streaming & PySpark) | Stream and batch transformations     |
| PostgreSQL                                    | Analytics database / data warehouse  |
| Docker                                        | Containerized deployment             |
| pgAdmin                                       | DB management interface              |

---

## 🎧 ETL/Streaming Overview

### 📥 1. Event Ingestion (Kafka)

Simulated real-time streaming events:

* `user_activity` (play, pause, skip, like events)
* `song_metadata`
* `artist_metadata`

### 🔄 2. Batch Transform (PySpark)

* Deduplication and standardization
* Song-user-session joins
* Feature enrichment and derived attributes
* Time-window aggregations (daily, weekly)

### 📤 3. Load (PostgreSQL)

Final analytical tables:

* `users_data`
* `songs_data`
* `artists_data`
---
## 🚀 How to Run the Project

### 1. Clone the Repo

```bash
git clone https://github.com/srvindukuri/StreamPulse_Analytics.git
cd StreamPulse_Analytics
```

### 2. Start Services

```bash
docker-compose build
docker-compose up --build
```

### 3. Access Web Interfaces

| Service | URL                                            | Credentials                                       |
| ------- | ---------------------------------------------- | ------------------------------------------------- |
| Airflow | [http://localhost:8080](http://localhost:8080) | admin / admin                                     |
| pgAdmin | [http://localhost:5050](http://localhost:5050) | [admin@admin.com](mailto:admin@admin.com) / admin |


### 4. Trigger Batch ETL in Airflow

Enable and run `streampulse_batch_etl_dag` in Airflow.

---

## 📊 Output Tables in PostgreSQL

| Table Name              | Description                                   |
| ----------------------- | --------------------------------------------- |
| users_data              | User attributes and profiles                  |
| songs_data              | Track metadata and duration                   |
| artists_data            | Artist metadata                               |

---

## 📈 Features Covered

* Spark Structured Streaming with watermarking and window joins
* Batch ETL and dimension modeling
* Airflow-orchestrated pipelines
* PostgreSQL warehouse with fact/dimension schema
* Docker-based end-to-end orchestration
* Scalable streaming architecture with checkpoints and Parquet sinks

---

## ✅ Next Improvements (Optional)

* Implement **Real-time Kafka event streaming** for Data Streaming
* Implement **Great Expectations / Deequ** for data quality checks
* Kafka Connect + Schema Registry (Avro / Protobuf)
* BI dashboards (Power BI / Tableau / Superset)
* Real-time OLAP store like ClickHouse / Druid

---

## 🎤 Author

**Ravi Varma Indukuri**
Data Engineer | Apache Airflow | PySpark | Real-Time Processing | PostgreSQL

