from kafka import KafkaConsumer
import json
import time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

# Kafka setup
BOOTSTRAP_SERVERS = 'localhost:9092'
TOPICS = ['users_profile', 'currency_rates', 'transactions_raw']

consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='sadc-enrichment-consumer',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Lookup tables
user_profiles = {}
currency_rates = {}

# Output buffers
buffer = []
last_flush = time.time()

# CSV alert file path
ALERT_CSV = "alerts.csv"

# Initialize alert CSV with headers if not exists
if not os.path.exists(ALERT_CSV):
    pd.DataFrame(columns=[
        "transaction_id", "user_id", "timestamp", "amount", "currency",
        "location_country", "location_city", "is_fraud_simulated",
        "home_country", "home_city", "rate_to_usd"
    ]).to_csv(ALERT_CSV, index=False)

# Fraud detection logic
def is_flagged(txn):
    user = txn.get("user_profile", {})
    location_mismatch = txn["location"]["country"] != user.get("home_country")
    inflated_amount = txn["amount"] > 5000
    rate = txn.get("rate_to_usd")
    low_rate = isinstance(rate, (int, float)) and rate < 0.001
    return location_mismatch or inflated_amount or low_rate

# Enrichment logic
def enrich_transaction(txn):
    user_id = txn['user_id']
    currency = txn['currency']

    user_info = user_profiles.get(user_id, {})
    rate_info = currency_rates.get(currency, {})

    enriched = {
        **txn,
        "user_profile": user_info,
        "rate_to_usd": rate_info.get("rate_to_usd"),
        "rate_timestamp": rate_info.get("timestamp")
    }
    return enriched

# Flatten enriched transaction for Parquet
def flatten(txn):
    user = txn.get("user_profile", {})
    return {
        "transaction_id": txn["transaction_id"],
        "user_id": txn["user_id"],
        "timestamp": txn["timestamp"],
        "amount": txn["amount"],
        "currency": txn["currency"],
        "location_country": txn["location"]["country"],
        "location_city": txn["location"]["city"],
        "is_fraud_simulated": txn["is_fraud_simulated"],
        "rate_to_usd": txn.get("rate_to_usd"),
        "rate_timestamp": txn.get("rate_timestamp"),
        "user_name": user.get("name"),
        "user_gender": user.get("gender"),
        "user_dob": user.get("date_of_birth"),
        "home_country": user.get("home_country"),
        "home_city": user.get("home_city")
    }

# Flush buffer to Parquet in current folder
def flush_to_parquet():
    global buffer
    if buffer:
        flat_records = [flatten(txn) for txn in buffer]
        df = pd.DataFrame(flat_records)
        table = pa.Table.from_pandas(df)
        timestamp = int(time.time())
        pq.write_table(table, f"transactions_{timestamp}.parquet")
        print(f"📝 Flushed {len(buffer)} records to transactions_{timestamp}.parquet")
        buffer.clear()

# Append flagged transaction to CSV
def write_alert_csv(txn):
    user = txn.get("user_profile", {})
    alert_row = {
        "transaction_id": txn["transaction_id"],
        "user_id": txn["user_id"],
        "timestamp": txn["timestamp"],
        "amount": txn["amount"],
        "currency": txn["currency"],
        "location_country": txn["location"]["country"],
        "location_city": txn["location"]["city"],
        "is_fraud_simulated": txn["is_fraud_simulated"],
        "home_country": user.get("home_country"),
        "home_city": user.get("home_city"),
        "rate_to_usd": txn.get("rate_to_usd")
    }
    pd.DataFrame([alert_row]).to_csv(ALERT_CSV, mode='a', header=False, index=False)
    print(f"🚨 Alert: {txn['transaction_id']} flagged and written to CSV")

# Main loop
if __name__ == "__main__":
    print("🚀 Enrichment consumer started...")
    for msg in consumer:
        topic = msg.topic
        data = msg.value

        if topic == 'users_profile':
            user_profiles[data['user_id']] = data

        elif topic == 'currency_rates':
            currency_rates[data['currency']] = data

        elif topic == 'transactions_raw':
            enriched_txn = enrich_transaction(data)
            buffer.append(enriched_txn)

            if is_flagged(enriched_txn):
                write_alert_csv(enriched_txn)

            if time.time() - last_flush >= 60:
                flush_to_parquet()
                last_flush = time.time()