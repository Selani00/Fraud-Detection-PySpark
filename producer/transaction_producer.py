#!/usr/bin/env python3
"""
FinTech Fraud Detection Pipeline — Transaction Producer
=======================================================
Simulates credit card transactions for a digital wallet provider.

Fraud injection (controlled, ~5% rate):
  - IMPOSSIBLE_TRAVEL: Same user makes transactions from 2 different countries
    within 10 minutes.
  - HIGH_VALUE: Single transaction amount > $5,000.

Logs: Clean, self-explanatory stdout output.
"""

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

from confluent_kafka import Producer, KafkaException

# ─────────────────────────────────────────────────────────────
# CONFIGURATION (from environment variables)
# ─────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
TRANSACTIONS_PER_SECOND = float(os.getenv("TRANSACTIONS_PER_SECOND", "2"))
NUM_USERS = int(os.getenv("NUM_USERS", "20"))
FRAUD_INJECTION_RATE = float(os.getenv("FRAUD_INJECTION_RATE", "0.05"))

# ─────────────────────────────────────────────────────────────
# SYNTHETIC DATA DEFINITIONS
# ─────────────────────────────────────────────────────────────
MERCHANT_CATEGORIES = [
    "Electronics", "Groceries", "Travel", "Dining",
    "Entertainment", "Healthcare", "Fuel", "Online_Retail"
]

# Locations by country code — for impossible-travel detection
LOCATIONS = {
    "LK": ["Colombo, LK", "Kandy, LK", "Galle, LK", "Negombo, LK"],
    "US": ["New York, US", "Los Angeles, US", "Chicago, US", "Miami, US"],
    "GB": ["London, GB", "Manchester, GB", "Birmingham, GB"],
    "SG": ["Singapore, SG"],
    "AU": ["Sydney, AU", "Melbourne, AU"],
    "AE": ["Dubai, AE", "Abu Dhabi, AE"],
    "IN": ["Mumbai, IN", "Bangalore, IN", "Delhi, IN"],
    "JP": ["Tokyo, JP", "Osaka, JP"],
}

# Normal transaction amount ranges by category (min, max)
AMOUNT_RANGES = {
    "Electronics":    (50,   2000),
    "Groceries":      (10,   300),
    "Travel":         (100,  3000),
    "Dining":         (5,    150),
    "Entertainment":  (10,   200),
    "Healthcare":     (20,   500),
    "Fuel":           (20,   120),
    "Online_Retail":  (15,   800),
}

# ─────────────────────────────────────────────────────────────
# USER POOL
# ─────────────────────────────────────────────────────────────
USERS = [f"user_{str(i).zfill(3)}" for i in range(1, NUM_USERS + 1)]

# Tracks the last location and timestamp for each user (for impossible-travel)
user_last_location: dict = {}


def get_country(location: str) -> str:
    """Extract country code from location string."""
    return location.split(", ")[-1]


def random_location(exclude_country: Optional[str] = None) -> str:
    """Pick a random location, optionally excluding a specific country."""
    countries = list(LOCATIONS.keys())
    if exclude_country:
        countries = [c for c in countries if c != exclude_country]
    country = random.choice(countries)
    return random.choice(LOCATIONS[country])


def normal_transaction(user_id: str) -> dict:
    """Generate a normal (non-fraud) transaction for a user."""
    category = random.choice(MERCHANT_CATEGORIES)
    min_amt, max_amt = AMOUNT_RANGES[category]
    amount = round(random.uniform(min_amt, max_amt), 2)

    # Use user's last known country for geographic consistency, or random
    last = user_last_location.get(user_id)
    if last and random.random() < 0.85:
        # 85% chance user stays in same country
        country = get_country(last["location"])
        location = random.choice(LOCATIONS[country])
    else:
        location = random_location()

    return {
        "transaction_id": f"txn_{uuid.uuid4().hex[:12]}",
        "user_id": user_id,
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "merchant_category": category,
        "amount": amount,
        "location": location,
        "currency": "USD",
        "is_fraud_injected": False,
        "fraud_type": None,
    }


def inject_high_value(user_id: str) -> dict:
    """Inject a HIGH_VALUE fraud transaction (amount > $5,000)."""
    category = random.choice(["Electronics", "Travel", "Online_Retail"])
    amount = round(random.uniform(5001, 25000), 2)
    location = random_location()

    return {
        "transaction_id": f"txn_{uuid.uuid4().hex[:12]}",
        "user_id": user_id,
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "merchant_category": category,
        "amount": amount,
        "location": location,
        "currency": "USD",
        "is_fraud_injected": True,
        "fraud_type": "HIGH_VALUE",
    }


def inject_impossible_travel(user_id: str) -> list:
    """
    Inject an IMPOSSIBLE_TRAVEL fraud scenario:
    Send two transactions from different countries with the same timestamp
    (simulating < 1 second apart, well within the 10-minute window).
    Returns a list of 2 transactions.
    """
    country1 = random.choice(list(LOCATIONS.keys()))
    location1 = random.choice(LOCATIONS[country1])

    country2 = random.choice([c for c in LOCATIONS if c != country1])
    location2 = random.choice(LOCATIONS[country2])

    base_time = datetime.now(timezone.utc)
    ts1 = base_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    ts2 = base_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    category = random.choice(MERCHANT_CATEGORIES)
    amount1 = round(random.uniform(50, 500), 2)
    amount2 = round(random.uniform(50, 500), 2)

    txn1 = {
        "transaction_id": f"txn_{uuid.uuid4().hex[:12]}",
        "user_id": user_id,
        "timestamp": ts1,
        "merchant_category": category,
        "amount": amount1,
        "location": location1,
        "currency": "USD",
        "is_fraud_injected": True,
        "fraud_type": "IMPOSSIBLE_TRAVEL",
    }

    txn2 = {
        "transaction_id": f"txn_{uuid.uuid4().hex[:12]}",
        "user_id": user_id,
        "timestamp": ts2,
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "amount": amount2,
        "location": location2,
        "currency": "USD",
        "is_fraud_injected": True,
        "fraud_type": "IMPOSSIBLE_TRAVEL",
    }

    return [txn1, txn2]


def delivery_report(err, msg):
    """Kafka delivery callback — logs success or error."""
    if err is not None:
        print(f"[KAFKA ERROR] Delivery failed: {err}")
    else:
        pass  # Silent on success to keep logs clean


def send_transaction(producer: Producer, transaction: dict):
    """Serialize and send a transaction to Kafka."""
    # Remove internal fields before sending
    payload = {k: v for k, v in transaction.items()
               if k not in ("is_fraud_injected", "fraud_type")}
    
    producer.produce(
        topic=KAFKA_TOPIC,
        key=transaction["user_id"].encode("utf-8"),
        value=json.dumps(payload).encode("utf-8"),
        callback=delivery_report,
    )

    # Update user's last known location
    user_last_location[transaction["user_id"]] = {
        "location": transaction["location"],
        "timestamp": transaction["timestamp"],
    }


def log_transaction(txn: dict, label: str = "NORMAL"):
    """Print a clean, self-explanatory log line."""
    ts = txn["timestamp"]
    user = txn["user_id"]
    amount = txn["amount"]
    location = txn["location"]
    category = txn["merchant_category"]
    tid = txn["transaction_id"]
    print(f"[{ts}] [{label:20s}] {tid} | {user} | ${amount:>10.2f} | {category:<15} | {location}")


def main():
    print("=" * 80)
    print("  FinTech Fraud Detection Pipeline — Transaction Producer")
    print(f"  Kafka: {KAFKA_BOOTSTRAP_SERVERS} → Topic: {KAFKA_TOPIC}")
    print(f"  Users: {NUM_USERS} | Rate: {TRANSACTIONS_PER_SECOND} TPS | Fraud Rate: {FRAUD_INJECTION_RATE:.0%}")
    print("=" * 80)

    producer_config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 500,
        "linger.ms": 50,         # Small batching for efficiency
        "batch.size": 16384,
    }

    # Wait for Kafka to be ready
    print("[INIT] Connecting to Kafka...")
    while True:
        try:
            producer = Producer(producer_config)
            # Test connection
            producer.list_topics(timeout=10)
            print("[INIT] Connected to Kafka successfully.")
            break
        except KafkaException as e:
            print(f"[INIT] Kafka not ready ({e}), retrying in 5s...")
            time.sleep(5)

    interval = 1.0 / TRANSACTIONS_PER_SECOND
    transaction_count = 0
    fraud_count = 0

    print(f"\n[START] Producing transactions... (Ctrl+C to stop)\n")
    print(f"{'Timestamp':<28} {'Type':<22} {'Txn ID':<18} {'User':<10} {'Amount':>12} {'Category':<16} Location")
    print("-" * 130)

    try:
        while True:
            user_id = random.choice(USERS)

            # Decide whether to inject fraud (controlled rate)
            inject_fraud = random.random() < FRAUD_INJECTION_RATE

            if inject_fraud:
                fraud_type = random.choice(["HIGH_VALUE", "IMPOSSIBLE_TRAVEL"])

                if fraud_type == "HIGH_VALUE":
                    txn = inject_high_value(user_id)
                    send_transaction(producer, txn)
                    log_transaction(txn, label="⚠ FRAUD:HIGH_VALUE")
                    fraud_count += 1
                    transaction_count += 1

                elif fraud_type == "IMPOSSIBLE_TRAVEL":
                    txns = inject_impossible_travel(user_id)
                    for txn in txns:
                        send_transaction(producer, txn)
                        log_transaction(txn, label="⚠ FRAUD:IMP_TRAVEL")
                        fraud_count += 1
                        transaction_count += 1
                    time.sleep(0.1)  # Small delay between pair

            else:
                txn = normal_transaction(user_id)
                send_transaction(producer, txn)
                log_transaction(txn, label="NORMAL")
                transaction_count += 1

            # Flush periodically
            if transaction_count % 10 == 0:
                producer.poll(0)

            # Stats summary every 100 transactions
            if transaction_count % 100 == 0:
                print(f"\n[STATS] Produced: {transaction_count} | Fraud injected: {fraud_count} ({fraud_count/transaction_count:.1%})\n")

            time.sleep(interval)

    except KeyboardInterrupt:
        print(f"\n\n[STOP] Shutting down producer...")
        print(f"[STATS] Total produced: {transaction_count} | Fraud injected: {fraud_count}")
    finally:
        producer.flush(timeout=10)
        print("[STOP] All messages flushed. Producer stopped.")


if __name__ == "__main__":
    main()
