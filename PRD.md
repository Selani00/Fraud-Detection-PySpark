# Product Requirements Document
# FinTech Fraud Detection Pipeline — EC8207 ABDA Mini Project

**Version:** 1.0  
**Date:** 2026-05-06  
**Module:** EC8207 Applied Big Data Engineering  
**Scenario:** Scenario 2 — FinTech Fraud Detection Pipeline  
**Architecture:** Lambda Architecture  

---

## 1. Executive Summary

This project implements a production-grade **Lambda Architecture** data pipeline for real-time fraud detection in a digital wallet system. The system ingests synthetic credit card transaction streams via Apache Kafka, processes them in real-time using Apache Spark Structured Streaming, flags fraudulent transactions immediately, and orchestrates batch ETL reporting via Apache Airflow every 6 hours.

---

## 2. Project Objectives

| Objective | Description |
|-----------|-------------|
| Real-Time Fraud Detection | Flag impossible-travel and high-value transactions within seconds of ingestion |
| Batch Reconciliation | Every 6 hours, generate a reconciliation report comparing ingress vs validated amounts |
| Analytical Reporting | Generate fraud-attempt analysis by merchant category |
| Compliance | Demonstrate data governance and privacy-by-design principles |

---

## 3. Architecture

### 3.1 Lambda Architecture Overview

```
[Python Producer]
      │
      ▼
[Apache Kafka]  ──────────────────────────────────────────────────────────┐
      │                                                                    │
      ▼  (Speed Layer)                                                     │  (Batch Layer)
[Spark Structured Streaming]                                       [Raw Topic / Parquet]
      │                                                                    │
      ├──► FRAUD ──► PostgreSQL (fraud_alerts table)               [Apache Airflow DAG]
      │                                                                    │
      └──► VALID ──► PostgreSQL (validated_transactions table)     ETL every 6 hours
                           │                                               │
                           └───────────────────────────────────────────────┘
                                                                           │
                                                                     [Reconciliation Report]
                                                                     [Fraud by Category CSV]
```

### 3.2 Component Roles

| Component | Role | Justification |
|-----------|------|---------------|
| **Apache Kafka 3.7** | Message broker / ingestion layer | Decouples producers from consumers, provides durable, ordered, partitioned streams. Industry standard for high-throughput event streaming. |
| **Apache Spark 3.5 Structured Streaming** | Stream processing (Speed Layer) | Unified batch+stream API, micro-batch engine with exactly-once semantics, native Kafka integration, windowing support for impossible-travel detection. |
| **Apache Airflow 2.9** | Orchestration (Batch Layer) | DAG-based scheduling, rich operator ecosystem, supports parameterised ETL runs every 6 hours, built-in retry and alerting. |
| **PostgreSQL 16** | Sink database | ACID-compliant, supports complex fraud analytics queries, easy local setup via Docker, well-supported by both Spark (JDBC) and Airflow. |
| **Docker Compose** | Local infrastructure | Reproducible environment, single command spin-up of Kafka, Spark, Airflow, and PostgreSQL. |
| **Python 3.11** | Producer & DAG scripts | Rich ecosystem (confluent-kafka, faker, psycopg2), clean async producer support. |

---

## 4. Data Model

### 4.1 Producer Output (Kafka Message — JSON)

```json
{
  "transaction_id": "txn_a1b2c3d4",
  "user_id": "user_042",
  "timestamp": "2026-05-06T14:32:01.123Z",
  "merchant_category": "Electronics",
  "amount": 4500.00,
  "location": "Colombo, LK",
  "currency": "USD"
}
```

**Merchant Categories:** Electronics, Groceries, Travel, Dining, Entertainment, Healthcare, Fuel, Online Retail

### 4.2 Fraud Detection Rules

| Rule | Condition | Action |
|------|-----------|--------|
| **Impossible Travel** | Same user_id makes transactions from 2 different countries within 10 minutes | Flag as FRAUD |
| **High-Value** | amount > $5,000 | Flag as FRAUD |

### 4.3 Database Schema

**Table: `transactions_raw`**
```sql
transaction_id   VARCHAR PRIMARY KEY
user_id          VARCHAR
event_timestamp  TIMESTAMP
merchant_category VARCHAR
amount           NUMERIC(12,2)
location         VARCHAR
currency         VARCHAR
ingested_at      TIMESTAMP DEFAULT NOW()
```

**Table: `fraud_alerts`**
```sql
alert_id         SERIAL PRIMARY KEY
transaction_id   VARCHAR
user_id          VARCHAR
fraud_reason     VARCHAR   -- 'IMPOSSIBLE_TRAVEL' | 'HIGH_VALUE'
amount           NUMERIC(12,2)
location         VARCHAR
merchant_category VARCHAR
detected_at      TIMESTAMP DEFAULT NOW()
```

**Table: `validated_transactions`**
```sql
transaction_id   VARCHAR PRIMARY KEY
user_id          VARCHAR
event_timestamp  TIMESTAMP
merchant_category VARCHAR
amount           NUMERIC(12,2)
location         VARCHAR
batch_processed_at TIMESTAMP
```

**Table: `reconciliation_reports`**
```sql
report_id        SERIAL PRIMARY KEY
window_start     TIMESTAMP
window_end       TIMESTAMP
total_ingress_count   INTEGER
total_ingress_amount  NUMERIC(14,2)
fraud_count      INTEGER
fraud_amount     NUMERIC(14,2)
validated_count  INTEGER
validated_amount NUMERIC(14,2)
generated_at     TIMESTAMP DEFAULT NOW()
```

---

## 5. Functional Requirements

### 5.1 Producer (FR-P)

| ID | Requirement |
|----|-------------|
| FR-P-01 | Generate transactions at ~1 transaction/second per simulated user |
| FR-P-02 | Support configurable number of users (default: 20) |
| FR-P-03 | Inject fraudulent transactions at ~5% rate (controlled, not random) |
| FR-P-04 | Fraud injection types: impossible-travel pairs and high-value single transactions |
| FR-P-05 | Publish to Kafka topic `transactions` with user_id as partition key |
| FR-P-06 | Log each produced message cleanly to stdout with timestamp and type |

### 5.2 Stream Processing (FR-S)

| ID | Requirement |
|----|-------------|
| FR-S-01 | Consume from Kafka topic `transactions` |
| FR-S-02 | Detect high-value fraud (amount > 5000) immediately per message |
| FR-S-03 | Detect impossible-travel fraud using stateful 10-minute session window per user_id |
| FR-S-04 | Write fraud records to PostgreSQL `fraud_alerts` table |
| FR-S-05 | Write valid records to PostgreSQL `validated_transactions` table |
| FR-S-06 | Handle late data up to 5 minutes (watermarking) |
| FR-S-07 | Use event time (transaction timestamp), not processing time |
| FR-S-08 | Log fraud detections to stdout with reason code |

### 5.3 Airflow Orchestration (FR-A)

| ID | Requirement |
|----|-------------|
| FR-A-01 | DAG `fraud_etl_pipeline` runs every 6 hours |
| FR-A-02 | Task 1: Extract raw transactions from `transactions_raw` for the current window |
| FR-A-03 | Task 2: Join with `fraud_alerts` to separate validated vs fraud records |
| FR-A-04 | Task 3: Write validated records to Parquet files in `/data/warehouse/` |
| FR-A-05 | Task 4: Compute reconciliation metrics (counts and amounts) |
| FR-A-06 | Task 5: Insert reconciliation record into `reconciliation_reports` table |
| FR-A-07 | Task 6: Generate CSV report `fraud_by_category_YYYY-MM-DD_HH.csv` |
| FR-A-08 | Retry failed tasks up to 2 times with 5-minute delay |

### 5.4 Analytical Report (FR-R)

| ID | Requirement |
|----|-------------|
| FR-R-01 | Produce a CSV summarising fraud attempts grouped by merchant_category |
| FR-R-02 | Produce a reconciliation summary: Total Ingress Amount vs Validated Amount |
| FR-R-03 | Reports stored in `/data/reports/` with timestamped filenames |

---

## 6. Non-Functional Requirements

| Category | Requirement |
|----------|-------------|
| **Latency** | Fraud alerts written to DB within 10 seconds of transaction receipt |
| **Throughput** | Handle up to 100 transactions/second |
| **Availability** | Kafka replication factor ≥ 1 (single-node dev), designed for RF=3 in production |
| **Reproducibility** | Single `docker compose up` launches full stack |
| **Observability** | All components log to stdout; Airflow UI available at localhost:8080 |

---

## 7. Event Time vs Processing Time

**Event Time** is the timestamp embedded in the transaction JSON (`timestamp` field), representing when the transaction actually occurred at the merchant terminal.

**Processing Time** is when Spark receives and processes the message from Kafka.

**Why this distinction matters:**

- Network delays, producer batching, and Kafka replication mean messages can arrive out-of-order or late.
- For the impossible-travel rule (10-minute window), using processing time would give incorrect results — a transaction that occurred 8 minutes ago but arrived 2 minutes late would be missed.
- Spark Structured Streaming uses **watermarking** (`withWatermark("event_timestamp", "5 minutes")`) to handle late data. Records arriving more than 5 minutes after the watermark are dropped.

**Implementation:**

```python
df.withWatermark("event_timestamp", "5 minutes") \
  .groupBy(window("event_timestamp", "10 minutes"), "user_id") \
  ...
```

This ensures fraud detection is based on **when transactions happened**, not when the system saw them.

---

## 8. Ethics & Data Governance

### 8.1 Privacy Implications

The pipeline processes **Personally Identifiable Financial Information (PIFI)**: user identifiers, transaction amounts, merchant categories, and geolocation data. This creates significant privacy risks:

| Risk | Description |
|------|-------------|
| **User Profiling** | Aggregated spending patterns reveal lifestyle, health conditions, political affiliation |
| **Location Tracking** | Transaction locations constitute movement surveillance |
| **Financial Discrimination** | Fraud scores could be used to discriminate against users unfairly |
| **Data Breach** | Centralised financial data is a high-value target |

### 8.2 Data Governance Measures

| Measure | Implementation |
|---------|----------------|
| **Data Minimisation** | Producer does not include card numbers, names, or full addresses — only synthetic IDs |
| **Purpose Limitation** | Data is used solely for fraud detection; no cross-purpose profiling |
| **Retention Policy** | Raw Kafka messages retained for 7 days only (configured via `log.retention.hours`) |
| **Access Control** | PostgreSQL credentials managed via Docker secrets / environment variables, never hardcoded |
| **Pseudonymisation** | user_id is a synthetic identifier — not directly linkable to real persons in this demo |
| **Audit Trail** | All fraud alerts timestamped with `detected_at`; reconciliation reports provide accountability |
| **Right to Erasure** | In production, a DELETE cascade on user_id across all tables would satisfy GDPR Article 17 |
| **Regulatory Alignment** | Architecture is compatible with GDPR, PCI-DSS (tokenisation of card data), and PDPA (Sri Lanka) requirements |

### 8.3 Ethical Considerations

- **Bias in Fraud Rules:** Rule-based systems (impossible travel, high-value thresholds) may disproportionately flag legitimate transactions from users who travel frequently or make large business purchases. In production, a feedback loop and human review queue are essential.
- **Transparency:** Users should be notified when their transaction is flagged, with a clear appeals process.
- **Model Governance:** Any ML enhancement to this pipeline must be audited for demographic bias before deployment.

---

## 9. Folder Structure

```
fraud-detection/
├── PRD.md                          # This document
├── docker-compose.yml              # Full stack orchestration
├── .env                            # Environment variables
├── README.md                       # Setup and run instructions
│
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── transaction_producer.py     # Kafka producer with controlled fraud injection
│
├── spark/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── fraud_detector.py           # Spark Structured Streaming job
│
├── airflow/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── dags/
│       └── fraud_etl_dag.py        # Airflow DAG (6-hour ETL + reports)
│
├── db/
│   └── init.sql                    # PostgreSQL schema initialization
│
├── data/
│   ├── warehouse/                  # Parquet validated transaction files
│   └── reports/                    # Generated CSV reports
│
└── architecture_diagram.png        # Visual architecture diagram
```

---

## 10. Tech Stack Versions (2026)

| Tool | Version | Image |
|------|---------|-------|
| Apache Kafka | 3.7.x | `apache/kafka:3.7.0` |
| Apache Spark | 3.5.x | `bitnami/spark:3.5` |
| Apache Airflow | 2.9.x | `apache/airflow:2.9.3` |
| PostgreSQL | 16 | `postgres:16-alpine` |
| Python | 3.11 | (base images) |
| confluent-kafka | 2.4.0 | pip |
| pyspark | 3.5.1 | pip |

---

## 11. Setup & Execution

### Prerequisites
- Docker Desktop ≥ 4.25 with Docker Compose v2
- 8 GB RAM minimum allocated to Docker
- Ports available: 2181, 9092, 5432, 8080, 7077, 8081, 4040

### Quick Start
```bash
git clone <repo>
cd fraud-detection
docker compose up -d
# Wait ~60 seconds for all services to be healthy
docker compose logs -f producer    # Watch transactions being generated
docker compose logs -f spark-streaming  # Watch fraud detections
# Open http://localhost:8080 for Airflow UI (admin/admin)
```

---

## 12. Deliverable Checklist

| Deliverable | Status |
|------------|--------|
| Architecture Diagram | ✅ Included in README |
| Python Producer | ✅ `producer/transaction_producer.py` |
| Spark Streaming Script | ✅ `spark/fraud_detector.py` |
| Airflow DAG | ✅ `airflow/dags/fraud_etl_dag.py` |
| Docker Compose | ✅ `docker-compose.yml` |
| Database Schema | ✅ `db/init.sql` |
| Reconciliation Report | ✅ Auto-generated CSV in `/data/reports/` |
| Fraud by Category Report | ✅ Auto-generated CSV in `/data/reports/` |
| Project Report (1500w) | ✅ See `REPORT.md` |
