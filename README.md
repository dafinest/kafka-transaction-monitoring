# Kafka Real-Time Fraud Detection System

A fraud detection system for SADC (Southern African Development Community) region transactions, built with Apache Kafka, featuring real-time stream processing, multi-rule fraud detection, and comprehensive monitoring.

## 📊 Project Overview

This system simulates a real-time financial transaction processing pipeline with intelligent fraud detection capabilities. It processes transactions from 16 SADC countries, enriches data with user profiles and currency rates and applies fraud detection rules to identify suspicious activities.

### Key Features

- **Real-time Stream Processing** 
- **Multi-Source Data Enrichment** 
- **7-Rule Fraud Detection Engine** 
- **Risk-Based Alerting** - Categorizes fraud by severity (LOW, MEDIUM, HIGH, CRITICAL)
- **Production-Ready Output** 


### Architecture
```
┌─────────────────┐     ┌─────────────────┐     ┌──────────────────┐
│   Producers     │────▶│  Kafka Topics   │────▶│    Consumer      │
├─────────────────┤     ├─────────────────┤     ├──────────────────┤
│ • Transactions  │     │ • transactions_ │     │ • Enrichment     │
│ • User Profiles │     │   raw           │     │ • Fraud Detection│
│ • Currency      │     │ • users_profile │     │ • Risk Scoring   │
│   Rates         │     │ • currency_     │     │ • Alerting       │
│                 │     │   rates         │     │                  │
└─────────────────┘     └─────────────────┘     └──────────────────┘
                                                          │
                                                          ▼
                                                 ┌──────────────────┐
                                                 │    Outputs       │
                                                 ├──────────────────┤
                                                 │ • Parquet Files  │
                                                 │ • Alert CSV      │
                                                 │ • Logs           │
                                                 └──────────────────┘
```

### Prerequisites
- Python 3.8+
- Apache Kafka 2.8+ (or 3.x)
- Zookeeper 3.6+

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/dafinest/kafka-transaction-monitoring.git
cd kafka-transaction-monitoring
```

2. **Install Python dependencies**
```bash
pip install -r requirements.txt
```

3. **Start Kafka & Zookeeper**

**Windows:**
```powershell
# Terminal 1: Start Zookeeper
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

# Terminal 2: Start Kafka
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

**Linux/Mac:**
```bash
# Terminal 1: Start Zookeeper
./bin/zookeeper-server-start.sh config/zookeeper.properties

# Terminal 2: Start Kafka
./bin/kafka-server-start.sh config/server.properties
```

4. **Create Kafka Topics**
```bash
# Transactions topic
kafka-topics.sh --create --topic transactions_raw --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# User profiles topic
kafka-topics.sh --create --topic users_profile --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# Currency rates topic
kafka-topics.sh --create --topic currency_rates --bootstrap-server localhost:9092 --partitions 2 --replication-factor 1
```

5. **Run the System**
```bash
# Terminal 3: Start transaction producer (generates continuous transactions)
cd producers/python
python transaction_raw_producer.py

# Terminal 4: Start user profile producer (loads 10,000 users)
python User_profile_producer.py

# Terminal 5: Start currency rate producer (loads SADC rates)
python currency_rates_producer.py

# Terminal 6: Start consumer (fraud detection engine)
cd consumers/python
python transaction_raw_consumer.py
```


**Schema:**
```
transaction_id, user_id, timestamp, amount, currency,
location_country, location_city, is_fraud_simulated,
rate_to_usd, user_name, user_gender, user_dob,
home_country, home_city, fraud_flags, risk_score, risk_level
```
## Key Concepts

- **Stream Processing** - Real-time data pipelines with Kafka
- **Data Enrichment** - Joining multiple data streams in real-time
- **Fraud Detection** - Rule-based systems with scoring algorithms
- **Data Partitioning** - Kafka topic partitioning for scalability
- **Consumer Groups** - Load balancing and fault tolerance
- **Columnar Storage** - Efficient analytics with Parquet


## Future Enhancements

- Add comprehensive unit tests (pytest)
- Implement Prometheus metrics export
- Add Grafana dashboards for monitoring
- Machine learning-based fraud detection
- Docker Compose setup for easy deployment
- Real-time dashboard with WebSocket
- Database persistence (PostgreSQL/MongoDB)
- API for fraud investigation
- Consumer lag monitoring