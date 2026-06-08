# EC8207 Applied Big Data Engineering — Project Report
# FinTech Fraud Detection Pipeline (Scenario 2)

**Word Count:** ~1,500 words

---

## 1. Introduction

This project implements a Lambda Architecture data pipeline for real-time fraud detection in a digital wallet provider's transaction system. The system ingests synthetic credit card transaction streams, detects fraudulent activity in real time, orchestrates batch ETL processes on a six-hour schedule, and produces analytical reconciliation reports. The pipeline demonstrates the core principles of modern big data engineering: fault tolerance, scalability, event-time semantics, and data governance.

---

## 2. Justification of Tools Selected

### 2.1 Apache Kafka 3.7 (Ingestion Layer)

Kafka was chosen as the ingestion backbone for three primary reasons. First, its log-based, partitioned architecture provides durable, replayable message storage — essential in financial systems where no transaction can be silently lost. Second, Kafka's producer key-based partitioning ensures that all transactions from the same `user_id` land in the same partition, enabling stateful stream processing without cross-partition shuffles. Third, Kafka's decoupling of producers from consumers means the Python transaction generator can run independently of the Spark processing job, allowing each to scale horizontally without coordination overhead.

Kafka 3.7 runs in **KRaft mode** (no ZooKeeper dependency), which simplifies deployment and aligns with the current production recommendation. The `transactions` topic is configured with four partitions and a seven-day log retention period — a deliberate data governance decision limiting how long raw financial data resides in the broker.

### 2.2 Apache Spark 3.5 Structured Streaming (Speed Layer)

Spark Structured Streaming was selected over Apache Storm for its unified batch-and-stream API, built-in exactly-once processing semantics (via checkpointing), and native Kafka source connector. The Structured Streaming model expresses both stateless filters (high-value detection) and stateful windowed aggregations (impossible-travel detection) using the same DataFrame API, reducing code complexity and testing surface.

Spark's `foreachBatch` sink pattern provides transactional control when writing to PostgreSQL — the batch can be retried if a JDBC write fails without reprocessing all upstream data, because Spark checkpointing tracks the last committed Kafka offset.

### 2.3 Apache Airflow 2.9 (Orchestration / Batch Layer)

Airflow was chosen to implement the batch layer of the Lambda architecture. Its DAG (Directed Acyclic Graph) model makes task dependencies explicit and auditable — every ETL run is logged with start time, duration, and status in Airflow's metadata database. The six-hour schedule (`0 */6 * * *`) aligns with typical financial reconciliation windows. Airflow's built-in retry logic (two retries with five-minute backoff) ensures transient database connectivity issues do not cause report gaps. XCom is used to pass metrics between tasks without storing intermediate data on disk unnecessarily.

### 2.4 PostgreSQL 16 (Storage Sink)

PostgreSQL was selected over Cassandra for this scenario because the analytical reporting requirements — aggregations, GROUP BY queries, JOIN operations between fraud and transaction tables — are inherently relational. Cassandra's wide-column model is optimised for key-value access at massive scale, but introduces significant complexity for ad-hoc analytical queries. PostgreSQL 16 supports JSON columns, range partitioning, and advanced analytics functions, while remaining straightforward to run in Docker for a development environment. In a production deployment scaling beyond tens of millions of daily transactions, the `validated_transactions` table would be migrated to a columnar store such as Amazon Redshift or Apache Iceberg on object storage, while PostgreSQL would remain the operational store for fraud alerts.

### 2.5 Docker Compose

Docker Compose v2 orchestrates the entire stack — Kafka, Spark master, Spark streaming worker, PostgreSQL, Airflow webserver, and Airflow scheduler — with a single `docker compose up --build` command. This reproducibility is essential for academic submission: the pipeline runs identically on any machine with Docker Desktop, eliminating environment-specific failures.

---

## 3. Event Time vs Processing Time

Financial fraud detection depends critically on the distinction between **event time** and **processing time**.

**Event time** is the timestamp embedded in the transaction JSON payload — the moment the transaction was authorised at the merchant terminal. **Processing time** is when Spark's micro-batch executor actually reads and processes the message from Kafka.

These can differ significantly in practice. A mobile payment made in an area with poor network connectivity may be buffered locally for several minutes before being uploaded and transmitted to Kafka. If processing time were used for the impossible-travel window, a transaction that genuinely occurred six minutes after a prior transaction in another country would appear to have occurred at the moment Spark processed it — potentially within the same micro-batch as a transaction that was delayed by two minutes — producing incorrect window boundaries and missed or false fraud signals.

This pipeline uses event time exclusively for the impossible-travel detection:

```python
df.withWatermark("event_timestamp", "5 minutes") \
  .groupBy(
      window("event_timestamp", "10 minutes"),
      col("user_id")
  ) \
  .agg(collect_set("country").alias("countries_seen"))
```

The **watermark** of five minutes tells Spark: "if a record arrives more than five minutes after the current watermark (the maximum observed event time minus the watermark delay), discard it — the window it belongs to has already been finalised." This bounded late-data tolerance prevents unbounded state accumulation while handling realistic network delays. For high-value fraud detection, event time vs processing time is irrelevant since the rule is purely per-record and stateless.

---

## 4. Ethics and Data Governance

### 4.1 Privacy Implications

The pipeline processes **Personally Identifiable Financial Information (PIFI)**: user identifiers, transaction amounts, merchant categories, and geolocation data. Even when user_id is a pseudonym (as in this synthetic implementation), geolocation data combined with transaction patterns constitutes a sensitive behavioural profile. Analysis of merchant categories across a user's transaction history can reveal health conditions (e.g., frequent pharmacy visits), political activity (e.g., donations), dietary choices, and financial stress indicators.

The **impossible-travel** detection specifically aggregates location data across time — effectively creating movement traces. In a production system deployed on real user data, this constitutes a form of passive surveillance that users may not have explicitly consented to at account creation.

There is also a risk of **discriminatory impact** from rule-based fraud detection. Thresholds calibrated on historical data from a particular demographic group may produce systematically higher false positive rates for other groups — for example, flagging legitimate high-value transactions from users in lower-income countries as suspicious. This is the "disparate impact" problem well-documented in algorithmic fairness literature.

### 4.2 Data Governance Framework

The following governance measures are implemented or specified in this pipeline:

**Data Minimisation:** The producer does not emit card numbers, full names, or national identity numbers. Only a synthetic `user_id`, merchant category, amount, and generalised location (city-level, not GPS coordinates) are transmitted. This aligns with GDPR Article 5(1)(c).

**Purpose Limitation:** Data flows strictly from producer → fraud detection → reporting. No cross-purpose profiling (e.g., building marketing segments) is implemented or enabled. This satisfies GDPR Article 5(1)(b).

**Retention Controls:** Kafka is configured with `log.retention.hours=168` (seven days). Raw transactions in PostgreSQL would be archived and purged on a rolling 90-day basis in production, with only aggregated statistics retained long-term.

**Access Control:** Database credentials are passed via Docker environment variables and never hardcoded in source files. In production, these would be managed by a secrets manager (e.g., HashiCorp Vault or AWS Secrets Manager).

**Audit Trail:** Every fraud alert is timestamped with `detected_at`. Every reconciliation report records the exact window it covers and when it was generated. This audit trail supports regulatory compliance (e.g., PCI-DSS requirement 10 — logging and monitoring).

**Right to Erasure (GDPR Article 17):** The schema is designed so that a `DELETE CASCADE` on `user_id` across `transactions_raw`, `fraud_alerts`, and `validated_transactions` would satisfy a data erasure request. Foreign key relationships make this a single SQL operation.

**Pseudonymisation:** In this demo, `user_id` values are synthetic identifiers (`user_001` through `user_020`). In a real deployment, the mapping between real customer accounts and pseudonymous processing IDs would be held in a separate, access-controlled key vault, separated from the analytics pipeline.

**Transparency:** Users of the real system should be notified via in-app messaging when a transaction is flagged, with a clear explanation of the applicable rule and an easy-to-use appeals process. Automated fraud flagging without human review violates the "right to explanation" principle (GDPR Article 22).

### 4.3 Regulatory Context

The architecture is designed to be compatible with: GDPR (EU Regulation 2016/679) for user data protection; PCI-DSS v4.0 for payment card data security (tokenisation of card numbers outside the pipeline boundary); and the Personal Data Protection Act (PDPA) of Sri Lanka for local regulatory compliance.

---

## 5. Conclusion

This Lambda Architecture pipeline demonstrates a complete, production-aligned approach to financial fraud detection. Kafka provides durable, partitioned ingestion; Spark Structured Streaming delivers sub-ten-second fraud alerts using event-time semantics and stateful windowing; Airflow orchestrates reproducible six-hour batch ETL cycles with full audit logging; and PostgreSQL serves as a reliable analytical sink. The ethics and governance framework embedded in the design — data minimisation, purpose limitation, pseudonymisation, and audit trails — reflects the standard of responsibility required when processing sensitive financial data in a real-world system.
