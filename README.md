# FinTech Fraud Detection Pipeline
**EC8207 Applied Big Data Engineering — Mini Project (Scenario 2)**

A production-grade **Lambda Architecture** pipeline for real-time fraud detection using Apache Kafka, Apache Spark Structured Streaming, Apache Airflow, and PostgreSQL.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        LAMBDA ARCHITECTURE                                  │
│                                                                             │
│  [Python Producer]                                                          │
│       │  (synthetic transactions + controlled fraud injection)              │
│       ▼                                                                     │
│  [Apache Kafka 3.7]  ←──────────────────────────────────────────────────┐  │
│   Topic: transactions                                                    │  │
│   Partitions: 4 | Retention: 7 days                                     │  │
│       │                                                                  │  │
│       ├─────────────────────────────────────────────────────────────┐   │  │
│       │  SPEED LAYER                                                 │   │  │
│       ▼                                                              │   │  │
│  [Spark Structured Streaming 3.5]                                    │   │  │
│   • HIGH_VALUE fraud:      per-message, 5s micro-batch               │   │  │
│   • IMPOSSIBLE_TRAVEL:     10-min event-time window, 30s trigger     │   │  │
│   • Watermark:             5 minutes (late data tolerance)           │   │  │
│       │                                                              │   │  │
│       ├──► fraud_alerts table (PostgreSQL)                           │   │  │
│       └──► validated_transactions table (PostgreSQL)                 │   │  │
│                                                                      │   │  │
│                                                                      │   │  │
│  BATCH LAYER                                                         │   │  │
│  [Apache Airflow 2.9] ──────────────────────────────────────────────┘   │  │
│   Schedule: every 6 hours                                                │  │
│   DAG: fraud_etl_pipeline                                               │  │
│                                                                          │  │
│   T1: extract_window_data       → Query last 6h transactions            │  │
│   T2: reconcile_fraud_vs_valid  → Compute fraud vs validated metrics    │  │
│   T3: write_parquet             → /data/warehouse/date=YYYY-MM-DD/      │  │
│   T4: insert_reconciliation     → reconciliation_reports table          │  │
│   T5: generate_fraud_report     → /data/reports/fraud_by_category_*.csv │  │
│   T6: generate_reconciliation   → /data/reports/reconciliation_*.csv    │  │
│                                                                          │  │
│  [PostgreSQL 16]                                                         │  │
│   • transactions_raw                                                     │  │
│   • fraud_alerts                                                         │  │
│   • validated_transactions                                               │  │
│   • reconciliation_reports                                               │  │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Prerequisites

| Requirement | Version |
|------------|---------|
| Docker Desktop | ≥ 4.25 |
| Docker Compose | v2 (built into Docker Desktop) |
| RAM allocated to Docker | ≥ 8 GB |
| Free disk space | ≥ 5 GB |

### Required open ports
`2181`, `9092`, `9093`, `5432`, `8080`, `8081`, `7077`, `4040`

---

## Quick Start

### 1. Clone and enter the project
```bash
git clone <repo-url>
cd fraud-detection
```

### 2. Create data directories
```bash
mkdir -p data/warehouse data/reports spark/checkpoints
```

### 3. Make the DB setup script executable
```bash
chmod +x db/create_multiple_dbs.sh
```

### 4. Build and start all services
```bash
docker compose up --build -d
```

### 5. Wait for services to be ready (~60-90 seconds)
```bash
docker compose ps        # All should show "healthy" or "running"
```

### 6. Monitor the pipeline
```bash
# Watch transactions being produced
docker compose logs -f producer

# Watch fraud detections in Spark
docker compose logs -f spark-streaming

# Airflow UI
open http://localhost:8080          # Login: admin / admin

# Spark UI
open http://localhost:8081          # Spark Master Web UI

# Spark Streaming UI
open http://localhost:4040          # Active streaming queries
```

---

## Trigger an Airflow Run Manually (for testing)

In the Airflow UI at http://localhost:8080:
1. Find DAG `fraud_etl_pipeline`
2. Click the **▶ Trigger** button
3. Watch tasks execute in sequence

Or via CLI:
```bash
docker exec airflow-scheduler airflow dags trigger fraud_etl_pipeline
```

---

## View Output Reports

```bash
# List generated reports
ls -la data/reports/

# View reconciliation report
cat data/reports/reconciliation_*.csv

# View fraud by category
cat data/reports/fraud_by_category_*.csv

# List Parquet warehouse files
find data/warehouse -name "*.parquet"
```

---

## Database Queries

```bash
# Connect to PostgreSQL
docker exec -it postgres psql -U appuser -d frauddb

# View recent fraud alerts
SELECT fraud_reason, merchant_category, amount, location, detected_at
FROM fraud_alerts
ORDER BY detected_at DESC
LIMIT 20;

# Fraud by category summary
SELECT * FROM fraud_by_category;

# Reconciliation history
SELECT * FROM reconciliation_summary;

# Transaction counts
SELECT COUNT(*) FROM transactions_raw;
SELECT COUNT(*) FROM fraud_alerts;
SELECT COUNT(*) FROM validated_transactions;
```

---

## Folder Structure

```
fraud-detection/
├── PRD.md                              # Product Requirements Document
├── docker-compose.yml                  # Full stack orchestration
├── README.md                           # This file
│
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── transaction_producer.py         # Kafka producer with controlled fraud injection
│
├── spark/
│   ├── Dockerfile
│   ├── checkpoints/                    # Spark checkpointing (created at runtime)
│   └── fraud_detector.py              # Spark Structured Streaming job
│
├── airflow/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── dags/
│       └── fraud_etl_dag.py           # Airflow DAG
│
├── db/
│   ├── init.sql                        # PostgreSQL schema
│   └── create_multiple_dbs.sh          # Multi-database init script
│
└── data/
    ├── warehouse/                      # Parquet files (created at runtime)
    └── reports/                        # CSV reports (created at runtime)
```

---

## Stopping the Stack

```bash
# Stop all services (preserve data)
docker compose down

# Stop and remove all volumes (clean slate)
docker compose down -v
```

---

## Tech Stack

| Component | Version | Purpose |
|-----------|---------|---------|
| Apache Kafka | 3.7.0 (KRaft mode) | Message ingestion broker |
| Apache Spark | 3.5 | Structured Streaming — fraud detection |
| Apache Airflow | 2.9.3 | Batch ETL orchestration |
| PostgreSQL | 16 | Fraud alerts + reconciliation storage |
| Python | 3.11 | Producer scripts + Airflow DAGs |
| Docker Compose | v2 | Local infrastructure |

---

## Fraud Detection Rules

| Rule | Threshold | Spark Method |
|------|-----------|-------------|
| HIGH_VALUE | amount > $5,000 | Per-row filter, 5s micro-batch |
| IMPOSSIBLE_TRAVEL | Same user in 2+ countries within 10 minutes | Stateful 10-min event-time window, watermark 5 min |

---

## Deliverables Checklist

- [x] Architecture Diagram (above)
- [x] Python Producer (`producer/transaction_producer.py`)
- [x] Spark Structured Streaming (`spark/fraud_detector.py`)
- [x] Airflow DAG (`airflow/dags/fraud_etl_dag.py`)
- [x] Docker Compose (`docker-compose.yml`)
- [x] Database Schema (`db/init.sql`)
- [x] Reconciliation Report (auto-generated CSV)
- [x] Fraud by Category Report (auto-generated CSV)
- [x] PRD (`PRD.md`)
