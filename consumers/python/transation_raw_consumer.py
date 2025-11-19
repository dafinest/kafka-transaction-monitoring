from kafka import KafkaConsumer
import json
import time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import logging
from fraud_detector import FraudDetector

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

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

# Initialize fraud detector
fraud_detector = FraudDetector(config={
    'high_amount': 5000,
    'low_rate': 0.001,
    'suspicious_hour_start': 1,
    'suspicious_hour_end': 5
})


user_profiles = {}
currency_rates = {}

# Output buffers
buffer = []
last_flush = time.time()


ALERT_CSV = "alerts.csv"

# Initialize alert CSV with NEW headers
if not os.path.exists(ALERT_CSV):
    pd.DataFrame(columns=[
        "transaction_id", "user_id", "timestamp", "amount", "currency",
        "location_country", "location_city", "is_fraud_simulated",
        "home_country", "home_city", "rate_to_usd",
        "fraud_flags", "risk_score", "risk_level" 
    ]).to_csv(ALERT_CSV, index=False)

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
        "home_city": user.get("home_city"),
        "fraud_flags": txn.get("fraud_flags", ""),
        "risk_score": txn.get("risk_score", 0),
        "risk_level": txn.get("risk_level", "")
    }

# Flush buffer to Parquet
def flush_to_parquet():
    global buffer
    if buffer:
        flat_records = [flatten(txn) for txn in buffer]
        df = pd.DataFrame(flat_records)
        table = pa.Table.from_pandas(df)
        timestamp = int(time.time())
        pq.write_table(table, f"transactions_{timestamp}.parquet")
        logger.info(f"Flushed {len(buffer)} records to transactions_{timestamp}.parquet")
        buffer.clear()

# Write alert to CSV with enhanced fraud data
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
        "rate_to_usd": txn.get("rate_to_usd"),
        "fraud_flags": ", ".join(txn.get("fraud_flags", [])),
        "risk_score": txn.get("risk_score", 0),
        "risk_level": txn.get("risk_level", "")
    }
    pd.DataFrame([alert_row]).to_csv(ALERT_CSV, mode='a', header=False, index=False)
    logger.warning(f"Alert: {txn['transaction_id']} | Risk: {alert_row['risk_level']} | Flags: {alert_row['fraud_flags']}")

# Main loop
if __name__ == "__main__":
    logger.info("Enrichment consumer started...")
    logger.info(f"Fraud detection thresholds: High amount={fraud_detector.HIGH_AMOUNT_THRESHOLD}, Low rate={fraud_detector.LOW_RATE_THRESHOLD}")
    
    try:
        for msg in consumer:
            topic = msg.topic
            data = msg.value

            if topic == 'users_profile':
                user_profiles[data['user_id']] = data
                logger.debug(f"Loaded user profile: {data['user_id']}")

            elif topic == 'currency_rates':
                currency_rates[data['currency']] = data
                logger.debug(f"Loaded currency rate: {data['currency']}")

            elif topic == 'transactions_raw':
                # Enrich transaction
                enriched_txn = enrich_transaction(data)
                
                # Run fraud detection
                is_fraud, fraud_flags, risk_score = fraud_detector.detect_fraud(enriched_txn)
                
                # Add fraud data to transaction
                enriched_txn["fraud_flags"] = fraud_flags
                enriched_txn["risk_score"] = risk_score
                enriched_txn["risk_level"] = fraud_detector.get_risk_level(risk_score)
                
                # Add to buffer
                buffer.append(enriched_txn)

                if is_fraud:
                    write_alert_csv(enriched_txn)

                # Flush to Parquet every 60 seconds
                if time.time() - last_flush >= 60:
                    flush_to_parquet()
                    last_flush = time.time()
    
    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user")
    finally:
        flush_to_parquet()  
        consumer.close()
        logger.info("Consumer shut down gracefully")