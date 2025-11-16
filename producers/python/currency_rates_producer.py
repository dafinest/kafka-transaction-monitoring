from kafka import KafkaProducer
from datetime import datetime
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Static rates (can be updated manually or via API later)
sadc_rates = {
    "AOA": 0.0015, "BWP": 0.074, "CDF": 0.0006, "ZWL": 0.0031,
    "ZAR": 0.055, "ZMW": 0.048, "TZS": 0.00039, "MWK": 0.00059,
    "LSL": 0.055, "SZL": 0.055, "MGA": 0.00022, "MZN": 0.015,
    "MUR": 0.022, "SCR": 0.074, "KMF": 0.0022, "NAD": 0.055
}

if __name__ == "__main__":
    try:
        for currency, rate in sadc_rates.items():
            record = {
                "currency": currency,
                "rate_to_usd": rate,
                "timestamp": datetime.utcnow().isoformat()
            }
            producer.send("currency_rates", record)
            print(f"Sent: {record}")
    finally:
        producer.flush(timeout=100000)
        producer.close()