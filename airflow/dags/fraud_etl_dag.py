"""
FinTech Fraud Detection Pipeline — Airflow DAG
===============================================
DAG: fraud_etl_pipeline
Schedule: Every 6 hours

Tasks:
  T1: extract_window_data      — Query raw transactions for last 6 hours
  T2: reconcile_fraud_vs_valid — Compute ingress vs fraud vs validated counts/amounts
  T3: write_parquet            — Write validated transactions to Parquet data warehouse
  T4: insert_reconciliation    — Persist reconciliation record to PostgreSQL
  T5: generate_fraud_report    — Generate CSV: fraud attempts by merchant category
  T6: generate_reconciliation_report — Generate CSV: ingress vs validated amounts

Data Governance:
  - All queries are time-windowed to the 6-hour batch window
  - No PII is written to report files (user_id is a pseudonym)
  - Parquet files are partitioned by date for efficient retention management
"""

import csv
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
DB_CONN_STRING = os.getenv(
    "APP_DB_CONN",
    "postgresql://appuser:apppassword@postgres:5432/frauddb"
)

REPORTS_DIR = Path("/data/reports")
WAREHOUSE_DIR = Path("/data/warehouse")
REPORTS_DIR.mkdir(parents=True, exist_ok=True)
WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)


def get_connection():
    """Return a psycopg2 connection to the fraud database."""
    return psycopg2.connect(DB_CONN_STRING)


# ─────────────────────────────────────────────────────────────
# DAG DEFAULT ARGUMENTS
# ─────────────────────────────────────────────────────────────
default_args = {
    "owner": "fraud-pipeline",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ─────────────────────────────────────────────────────────────
# TASK FUNCTIONS
# ─────────────────────────────────────────────────────────────

def extract_window_data(**context):
    """
    T1: Extract raw transactions for the current 6-hour window.
    Pushes summary statistics to XCom for downstream tasks.
    """
    logical_date = context["logical_date"]
    window_end = logical_date
    window_start = window_end - timedelta(hours=6)

    print(f"[T1] Extracting window: {window_start} → {window_end}")

    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    COUNT(*)                AS total_count,
                    COALESCE(SUM(amount), 0) AS total_amount
                FROM transactions_raw
                WHERE event_timestamp >= %s
                  AND event_timestamp <  %s
            """, (window_start, window_end))
            result = cur.fetchone()

        total_count = int(result["total_count"])
        total_amount = float(result["total_amount"])

        print(f"[T1] Window summary: {total_count} transactions, ${total_amount:,.2f} total")

        context["task_instance"].xcom_push("window_start", str(window_start))
        context["task_instance"].xcom_push("window_end", str(window_end))
        context["task_instance"].xcom_push("total_ingress_count", total_count)
        context["task_instance"].xcom_push("total_ingress_amount", total_amount)

    finally:
        conn.close()


def reconcile_fraud_vs_valid(**context):
    """
    T2: Compare fraud alerts vs validated transactions in the window.
    Pushes reconciliation metrics to XCom.
    """
    ti = context["task_instance"]
    window_start = ti.xcom_pull(task_ids="extract_window_data", key="window_start")
    window_end = ti.xcom_pull(task_ids="extract_window_data", key="window_end")
    total_count = ti.xcom_pull(task_ids="extract_window_data", key="total_ingress_count")
    total_amount = ti.xcom_pull(task_ids="extract_window_data", key="total_ingress_amount")

    print(f"[T2] Reconciling window: {window_start} → {window_end}")

    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Fraud stats
            cur.execute("""
                SELECT
                    COUNT(DISTINCT transaction_id) AS fraud_count,
                    COALESCE(SUM(amount), 0)       AS fraud_amount
                FROM fraud_alerts
                WHERE detected_at >= %s
                  AND detected_at <  %s
            """, (window_start, window_end))
            fraud_result = cur.fetchone()

            # Validated stats
            cur.execute("""
                SELECT
                    COUNT(*)                AS validated_count,
                    COALESCE(SUM(amount), 0) AS validated_amount
                FROM validated_transactions
                WHERE batch_processed_at >= %s
                  AND batch_processed_at <  %s
            """, (window_start, window_end))
            valid_result = cur.fetchone()

        fraud_count = int(fraud_result["fraud_count"])
        fraud_amount = float(fraud_result["fraud_amount"])
        validated_count = int(valid_result["validated_count"])
        validated_amount = float(valid_result["validated_amount"])

        print(f"[T2] Fraud:     {fraud_count} transactions, ${fraud_amount:,.2f}")
        print(f"[T2] Validated: {validated_count} transactions, ${validated_amount:,.2f}")
        print(f"[T2] Ingress:   {total_count} transactions, ${total_amount:,.2f}")

        ti.xcom_push("fraud_count", fraud_count)
        ti.xcom_push("fraud_amount", fraud_amount)
        ti.xcom_push("validated_count", validated_count)
        ti.xcom_push("validated_amount", validated_amount)

    finally:
        conn.close()


def write_parquet(**context):
    """
    T3: Write validated transactions to Parquet files (Data Warehouse layer).
    Files are partitioned by date: /data/warehouse/date=YYYY-MM-DD/batch_HH.parquet
    """
    ti = context["task_instance"]
    window_start = ti.xcom_pull(task_ids="extract_window_data", key="window_start")
    window_end = ti.xcom_pull(task_ids="extract_window_data", key="window_end")

    print(f"[T3] Writing Parquet for window: {window_start} → {window_end}")

    conn = get_connection()
    try:
        df = pd.read_sql("""
            SELECT
                transaction_id,
                user_id,
                event_timestamp,
                merchant_category,
                amount,
                location,
                currency,
                batch_processed_at
            FROM validated_transactions
            WHERE batch_processed_at >= %(start)s
              AND batch_processed_at <  %(end)s
        """, conn, params={"start": window_start, "end": window_end})
    finally:
        conn.close()

    if df.empty:
        print("[T3] No validated transactions in this window — skipping Parquet write.")
        return

    # Partition by date
    date_str = window_start[:10]  # YYYY-MM-DD
    hour_str = window_start[11:13]  # HH
    partition_dir = WAREHOUSE_DIR / f"date={date_str}"
    partition_dir.mkdir(parents=True, exist_ok=True)

    parquet_path = partition_dir / f"validated_batch_{hour_str}.parquet"
    df.to_parquet(parquet_path, index=False, engine="pyarrow")

    print(f"[T3] Wrote {len(df)} records to {parquet_path}")
    ti.xcom_push("parquet_path", str(parquet_path))
    ti.xcom_push("parquet_row_count", len(df))


def insert_reconciliation(**context):
    """
    T4: Insert reconciliation record into PostgreSQL.
    """
    ti = context["task_instance"]
    window_start = ti.xcom_pull(task_ids="extract_window_data", key="window_start")
    window_end = ti.xcom_pull(task_ids="extract_window_data", key="window_end")
    total_count = ti.xcom_pull(task_ids="extract_window_data", key="total_ingress_count")
    total_amount = ti.xcom_pull(task_ids="extract_window_data", key="total_ingress_amount")
    fraud_count = ti.xcom_pull(task_ids="reconcile_fraud_vs_valid", key="fraud_count")
    fraud_amount = ti.xcom_pull(task_ids="reconcile_fraud_vs_valid", key="fraud_amount")
    validated_count = ti.xcom_pull(task_ids="reconcile_fraud_vs_valid", key="validated_count")
    validated_amount = ti.xcom_pull(task_ids="reconcile_fraud_vs_valid", key="validated_amount")

    print(f"[T4] Inserting reconciliation record...")

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO reconciliation_reports
                    (window_start, window_end,
                     total_ingress_count, total_ingress_amount,
                     fraud_count, fraud_amount,
                     validated_count, validated_amount)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING report_id
            """, (
                window_start, window_end,
                total_count, total_amount,
                fraud_count, fraud_amount,
                validated_count, validated_amount,
            ))
            report_id = cur.fetchone()[0]
            conn.commit()

        print(f"[T4] Reconciliation report #{report_id} inserted successfully.")
        ti.xcom_push("report_id", report_id)

    finally:
        conn.close()


def generate_fraud_report(**context):
    """
    T5: Generate CSV report — Fraud Attempts by Merchant Category.
    Output: /data/reports/fraud_by_category_YYYY-MM-DD_HH.csv
    """
    ti = context["task_instance"]
    window_start = ti.xcom_pull(task_ids="extract_window_data", key="window_start")
    window_end = ti.xcom_pull(task_ids="extract_window_data", key="window_end")

    print(f"[T5] Generating fraud-by-category report...")

    conn = get_connection()
    try:
        df = pd.read_sql("""
            SELECT
                merchant_category,
                fraud_reason,
                COUNT(*)            AS attempt_count,
                SUM(amount)         AS total_amount,
                AVG(amount)         AS avg_amount,
                MAX(amount)         AS max_amount
            FROM fraud_alerts
            WHERE detected_at >= %(start)s
              AND detected_at <  %(end)s
            GROUP BY merchant_category, fraud_reason
            ORDER BY attempt_count DESC
        """, conn, params={"start": window_start, "end": window_end})
    finally:
        conn.close()

    date_str = window_start[:10]
    hour_str = window_start[11:13]
    report_path = REPORTS_DIR / f"fraud_by_category_{date_str}_{hour_str}.csv"

    if df.empty:
        # Write header-only file
        df = pd.DataFrame(columns=[
            "merchant_category", "fraud_reason", "attempt_count",
            "total_amount", "avg_amount", "max_amount"
        ])
        print("[T5] No fraud detected in this window — writing empty report.")
    else:
        df["total_amount"] = df["total_amount"].round(2)
        df["avg_amount"] = df["avg_amount"].round(2)
        df["max_amount"] = df["max_amount"].round(2)
        print(f"[T5] Fraud report summary:")
        print(df.to_string(index=False))

    df.to_csv(report_path, index=False)
    print(f"[T5] Report written to: {report_path}")
    ti.xcom_push("fraud_report_path", str(report_path))


def generate_reconciliation_report(**context):
    """
    T6: Generate CSV reconciliation report — Total Ingress vs Validated Amount.
    Output: /data/reports/reconciliation_YYYY-MM-DD_HH.csv
    """
    ti = context["task_instance"]
    window_start = ti.xcom_pull(task_ids="extract_window_data", key="window_start")
    window_end = ti.xcom_pull(task_ids="extract_window_data", key="window_end")
    total_count = ti.xcom_pull(task_ids="extract_window_data", key="total_ingress_count") or 0
    total_amount = ti.xcom_pull(task_ids="extract_window_data", key="total_ingress_amount") or 0
    fraud_count = ti.xcom_pull(task_ids="reconcile_fraud_vs_valid", key="fraud_count") or 0
    fraud_amount = ti.xcom_pull(task_ids="reconcile_fraud_vs_valid", key="fraud_amount") or 0
    validated_count = ti.xcom_pull(task_ids="reconcile_fraud_vs_valid", key="validated_count") or 0
    validated_amount = ti.xcom_pull(task_ids="reconcile_fraud_vs_valid", key="validated_amount") or 0
    report_id = ti.xcom_pull(task_ids="insert_reconciliation", key="report_id")

    fraud_pct = round((fraud_amount / total_amount * 100) if total_amount > 0 else 0, 2)
    valid_pct = round((validated_amount / total_amount * 100) if total_amount > 0 else 0, 2)

    summary = {
        "Report ID": report_id,
        "Window Start": window_start,
        "Window End": window_end,
        "Total Ingress Transactions": total_count,
        "Total Ingress Amount (USD)": round(total_amount, 2),
        "Fraud Transactions": fraud_count,
        "Fraud Amount (USD)": round(fraud_amount, 2),
        "Fraud Amount %": fraud_pct,
        "Validated Transactions": validated_count,
        "Validated Amount (USD)": round(validated_amount, 2),
        "Validated Amount %": valid_pct,
    }

    date_str = window_start[:10]
    hour_str = window_start[11:13]
    report_path = REPORTS_DIR / f"reconciliation_{date_str}_{hour_str}.csv"

    with open(report_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=summary.keys())
        writer.writeheader()
        writer.writerow(summary)

    print(f"[T6] Reconciliation Report:")
    for k, v in summary.items():
        print(f"       {k}: {v}")
    print(f"[T6] Report written to: {report_path}")


# ─────────────────────────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────────────────────────
with DAG(
    dag_id="fraud_etl_pipeline",
    description="FinTech Fraud Detection — ETL and reconciliation every 6 hours",
    default_args=default_args,
    schedule_interval="0 */6 * * *",   # Every 6 hours
    start_date=days_ago(1),
    catchup=False,
    tags=["fraud-detection", "fintech", "etl"],
    doc_md="""
## Fraud ETL Pipeline

Runs every 6 hours. Tasks:
1. `extract_window_data` — Query raw transactions for the last 6 hours
2. `reconcile_fraud_vs_valid` — Compute fraud vs validated metrics
3. `write_parquet` — Persist validated records to Parquet (Data Warehouse)
4. `insert_reconciliation` — Save reconciliation record to PostgreSQL
5. `generate_fraud_report` — CSV: fraud attempts by merchant category
6. `generate_reconciliation_report` — CSV: ingress vs validated amounts
    """,
) as dag:

    t1_extract = PythonOperator(
        task_id="extract_window_data",
        python_callable=extract_window_data,
        provide_context=True,
    )

    t2_reconcile = PythonOperator(
        task_id="reconcile_fraud_vs_valid",
        python_callable=reconcile_fraud_vs_valid,
        provide_context=True,
    )

    t3_parquet = PythonOperator(
        task_id="write_parquet",
        python_callable=write_parquet,
        provide_context=True,
    )

    t4_insert = PythonOperator(
        task_id="insert_reconciliation",
        python_callable=insert_reconciliation,
        provide_context=True,
    )

    t5_fraud_report = PythonOperator(
        task_id="generate_fraud_report",
        python_callable=generate_fraud_report,
        provide_context=True,
    )

    t6_reconciliation_report = PythonOperator(
        task_id="generate_reconciliation_report",
        python_callable=generate_reconciliation_report,
        provide_context=True,
    )

    # ── Task Dependencies ──────────────────────────────────────
    # T1 → T2 → T3 → T4 → T5
    #                  └──→ T6
    t1_extract >> t2_reconcile >> t3_parquet >> t4_insert >> [t5_fraud_report, t6_reconciliation_report]
