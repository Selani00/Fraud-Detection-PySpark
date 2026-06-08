#!/usr/bin/env python3
"""
FinTech Fraud Detection Pipeline — Spark Structured Streaming Job
=================================================================
Consumes transactions from Kafka and applies two fraud detection rules:

  1. HIGH_VALUE:         amount > $5,000 → flag immediately per message
  2. IMPOSSIBLE_TRAVEL:  same user_id appears in 2+ different countries
                         within a 10-minute event-time window

Event Time vs Processing Time:
  - Uses `timestamp` field from the JSON payload (event time) for all windows.
  - Watermark of 5 minutes allows Spark to handle late-arriving data.
  - Windows are based on WHEN transactions occurred, not when Spark saw them.

Sinks:
  - fraud_alerts table         → PostgreSQL (via JDBC, foreachBatch)
  - validated_transactions table → PostgreSQL (via JDBC, foreachBatch)
"""

import json
import os
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
POSTGRES_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://localhost:5432/frauddb")
POSTGRES_USER = os.getenv("POSTGRES_USER", "appuser")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "apppassword")
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
CHECKPOINT_DIR = "/tmp/spark-checkpoints"

# Fraud thresholds
HIGH_VALUE_THRESHOLD = 5000.0
TRAVEL_WINDOW_MINUTES = 10
WATERMARK_MINUTES = 5

# JDBC connection properties
JDBC_PROPS = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}

# ─────────────────────────────────────────────────────────────
# TRANSACTION JSON SCHEMA
# ─────────────────────────────────────────────────────────────
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id",    StringType(),    True),
    StructField("user_id",           StringType(),    True),
    StructField("timestamp",         StringType(),    True),  # parsed to timestamp below
    StructField("merchant_category", StringType(),    True),
    StructField("amount",            DoubleType(),    True),
    StructField("location",          StringType(),    True),
    StructField("currency",          StringType(),    True),
])


def create_spark_session() -> SparkSession:
    """Build and return a SparkSession with Kafka and PostgreSQL support."""
    return (
        SparkSession.builder
        .appName("FraudDetectionPipeline")
        .master(SPARK_MASTER)
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                "org.postgresql:postgresql:42.7.3")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
        .config("spark.sql.shuffle.partitions", "4")
        # Reduce noisy Spark logs
        .config("spark.sql.streaming.metricsEnabled", "true")
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession) -> DataFrame:
    """Read raw Kafka messages and parse JSON into structured columns."""
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("kafka.group.id", "fraud-detector-spark")
        .load()
    )

    # Deserialize JSON value
    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), TRANSACTION_SCHEMA).alias("data"),
        F.col("timestamp").alias("kafka_timestamp"),   # Kafka processing time (not used for windows)
    ).select("data.*", "kafka_timestamp")

    # Convert ISO timestamp string → proper event timestamp (event time)
    transactions = parsed.withColumn(
        "event_timestamp",
        F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    ).drop("timestamp")

    # Extract country code from location (e.g., "Colombo, LK" → "LK")
    transactions = transactions.withColumn(
        "country",
        F.trim(F.element_at(F.split(F.col("location"), ", "), -1))
    )

    return transactions


# ─────────────────────────────────────────────────────────────
# FRAUD DETECTION — HIGH VALUE (stateless, per-row)
# ─────────────────────────────────────────────────────────────
def detect_high_value(df: DataFrame) -> DataFrame:
    """Return rows where amount > HIGH_VALUE_THRESHOLD."""
    return df.filter(F.col("amount") > HIGH_VALUE_THRESHOLD)


# ─────────────────────────────────────────────────────────────
# FRAUD DETECTION — IMPOSSIBLE TRAVEL (stateful, windowed)
# ─────────────────────────────────────────────────────────────
def detect_impossible_travel(df: DataFrame) -> DataFrame:
    """
    Within each 10-minute event-time window, if a user_id appears in
    2 or more distinct countries, flag ALL their transactions in that window.

    Uses watermark to handle late data (up to 5 minutes late).
    """
    windowed = (
        df
        .withWatermark("event_timestamp", f"{WATERMARK_MINUTES} minutes")
        .groupBy(
            F.window("event_timestamp", f"{TRAVEL_WINDOW_MINUTES} minutes"),
            F.col("user_id")
        )
        .agg(
            F.collect_set("country").alias("countries_seen"),
            F.collect_list("transaction_id").alias("transaction_ids"),
            F.collect_list("location").alias("locations"),
            F.collect_list("merchant_category").alias("categories"),
            F.collect_list("amount").alias("amounts"),
            F.collect_list("event_timestamp").alias("event_timestamps"),
        )
        .filter(F.size("countries_seen") > 1)   # 2+ countries = impossible travel
    )

    # Explode back to individual transaction records for writing
    exploded = (
        windowed
        .select(
            F.col("user_id"),
            F.posexplode(F.col("transaction_ids")).alias("pos", "transaction_id"),
            F.col("locations"),
            F.col("categories"),
            F.col("amounts"),
            F.col("event_timestamps"),
            F.col("countries_seen"),
        )
        .select(
            F.col("transaction_id"),
            F.col("user_id"),
            F.col("locations").getItem(F.col("pos")).alias("location"),
            F.col("categories").getItem(F.col("pos")).alias("merchant_category"),
            F.col("amounts").getItem(F.col("pos")).alias("amount"),
            F.col("event_timestamps").getItem(F.col("pos")).alias("event_timestamp"),
        )
    )

    return exploded


# ─────────────────────────────────────────────────────────────
# BATCH WRITER FUNCTIONS (used with foreachBatch)
# ─────────────────────────────────────────────────────────────
def write_fraud_high_value(batch_df: DataFrame, batch_id: int):
    """Write HIGH_VALUE fraud alerts to PostgreSQL."""
    if batch_df.rdd.isEmpty():
        return

    count = batch_df.count()
    print(f"\n[FRAUD:HIGH_VALUE] Batch {batch_id} — {count} alert(s) detected")

    alert_df = batch_df.select(
        F.col("transaction_id"),
        F.col("user_id"),
        F.lit("HIGH_VALUE").alias("fraud_reason"),
        F.col("amount"),
        F.col("location"),
        F.col("merchant_category"),
        F.col("event_timestamp"),
    )

    # Log each fraud alert
    for row in alert_df.collect():
        print(f"  ► ALERT | {row.transaction_id} | {row.user_id} | ${row.amount:,.2f} | {row.location}")

    (
        alert_df.write
        .mode("append")
        .jdbc(POSTGRES_URL, "fraud_alerts", properties=JDBC_PROPS)
    )

    # Also write to raw transactions table (ignore duplicates)
    raw_df = batch_df.select(
        "transaction_id", "user_id", "event_timestamp",
        "merchant_category", "amount", "location", "currency"
    )
    try:
        raw_df.write.mode("append").jdbc(POSTGRES_URL, "transactions_raw", properties=JDBC_PROPS)
    except Exception as e:
        if "duplicate key" in str(e) or "unique constraint" in str(e):
            print(f"  ⚠ Duplicate entries in transactions_raw (expected with watermarking), skipping.")
        else:
            raise


def write_fraud_impossible_travel(batch_df: DataFrame, batch_id: int):
    """Write IMPOSSIBLE_TRAVEL fraud alerts to PostgreSQL."""
    if batch_df.rdd.isEmpty():
        return

    count = batch_df.count()
    print(f"\n[FRAUD:IMPOSSIBLE_TRAVEL] Batch {batch_id} — {count} record(s) flagged")

    alert_df = batch_df.select(
        F.col("transaction_id"),
        F.col("user_id"),
        F.lit("IMPOSSIBLE_TRAVEL").alias("fraud_reason"),
        F.col("amount"),
        F.col("location"),
        F.col("merchant_category"),
        F.col("event_timestamp"),
    )

    for row in alert_df.collect():
        print(f"  ► TRAVEL ALERT | {row.transaction_id} | {row.user_id} | {row.location}")

    try:
        (
            alert_df.write
            .mode("append")
            .jdbc(POSTGRES_URL, "fraud_alerts", properties=JDBC_PROPS)
        )
    except Exception as e:
        if "duplicate key" in str(e) or "unique constraint" in str(e):
            print(f"  ⚠ Duplicate fraud alerts (expected with windowing), skipping.")
        else:
            raise


def write_validated(batch_df: DataFrame, batch_id: int):
    """Write non-fraud (validated) transactions to PostgreSQL and raw table."""
    if batch_df.rdd.isEmpty():
        return

    count = batch_df.count()
    print(f"[VALIDATED] Batch {batch_id} — {count} transaction(s) validated")

    # Write to validated_transactions (ignore duplicates due to replays)
    validated_df = batch_df.select(
        "transaction_id", "user_id", "event_timestamp",
        "merchant_category", "amount", "location", "currency"
    )

    try:
        (
            validated_df.write
            .mode("append")
            .jdbc(POSTGRES_URL, "validated_transactions", properties=JDBC_PROPS)
        )
    except Exception as e:
        if "duplicate key" in str(e) or "unique constraint" in str(e):
            print(f"  ⚠ Some duplicate validated transactions (expected), continuing.")
        else:
            raise

    # Write to raw transactions table (ignore duplicates)
    try:
        (
            validated_df.write
            .mode("append")
            .jdbc(POSTGRES_URL, "transactions_raw", properties=JDBC_PROPS)
        )
    except Exception as e:
        if "duplicate key" in str(e) or "unique constraint" in str(e):
            print(f"  ⚠ Some duplicate transactions in raw table (expected), continuing.")
        else:
            raise


def main():
    print("=" * 80)
    print("  FinTech Fraud Detection — Spark Structured Streaming")
    print(f"  Kafka: {KAFKA_BOOTSTRAP_SERVERS} | Topic: {KAFKA_TOPIC}")
    print(f"  PostgreSQL: {POSTGRES_URL}")
    print(f"  High-Value Threshold: ${HIGH_VALUE_THRESHOLD:,.0f}")
    print(f"  Travel Window: {TRAVEL_WINDOW_MINUTES} min | Watermark: {WATERMARK_MINUTES} min")
    print("=" * 80)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")  # Reduce Spark noise

    print("\n[INIT] SparkSession created. Reading from Kafka...\n")

    # Read and parse stream
    transactions = read_kafka_stream(spark)

    # ── Stream 1: HIGH_VALUE fraud detection ──────────────────────────────────
    high_value_fraud = detect_high_value(transactions)

    query_high_value = (
        high_value_fraud.writeStream
        .outputMode("append")
        .foreachBatch(write_fraud_high_value)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/high_value")
        .trigger(processingTime="5 seconds")
        .start()
    )

    # ── Stream 2: IMPOSSIBLE_TRAVEL detection (windowed, stateful) ────────────
    impossible_travel_fraud = detect_impossible_travel(transactions)

    query_impossible_travel = (
        impossible_travel_fraud.writeStream
        .outputMode("update")           # Update mode for aggregations
        .foreachBatch(write_fraud_impossible_travel)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/impossible_travel")
        .trigger(processingTime="30 seconds")   # Check every 30s for window results
        .start()
    )

    # ── Stream 3: Validated transactions (non-fraud) ──────────────────────────
    # Filter out high-value fraud; impossible travel is handled separately
    validated = transactions.filter(F.col("amount") <= HIGH_VALUE_THRESHOLD)

    query_validated = (
        validated.writeStream
        .outputMode("append")
        .foreachBatch(write_validated)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/validated")
        .trigger(processingTime="5 seconds")
        .start()
    )

    print("[START] All streaming queries active.")
    print("  - Query 1: High-Value fraud detection (5s micro-batch)")
    print("  - Query 2: Impossible-Travel detection (30s windowed)")
    print("  - Query 3: Validated transaction writer (5s micro-batch)")
    print("\n[LISTENING] Waiting for transactions...\n")

    # Wait for all queries to terminate (runs indefinitely until stopped)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
