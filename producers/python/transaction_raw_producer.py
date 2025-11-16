import random
import time
import json
import uuid
from datetime import datetime
from kafka import KafkaProducer

# SADC country data
sadc_data = {
    "AO": {"name": "Angola", "currency": "AOA", "cities": ["Luanda", "Huambo", "Lubango"]},
    "BW": {"name": "Botswana", "currency": "BWP", "cities": ["Gaborone", "Francistown", "Maun"]},
    "CD": {"name": "DRC", "currency": "CDF", "cities": ["Kinshasa", "Lubumbashi", "Goma"]},
    "ZW": {"name": "Zimbabwe", "currency": "ZWL", "cities": ["Harare", "Bulawayo", "Mutare"]},
    "ZA": {"name": "South Africa", "currency": "ZAR", "cities": ["Johannesburg", "Cape Town", "Durban"]},
    "ZM": {"name": "Zambia", "currency": "ZMW", "cities": ["Lusaka", "Ndola", "Kitwe"]},
    "TZ": {"name": "Tanzania", "currency": "TZS", "cities": ["Dodoma", "Dar es Salaam", "Arusha"]},
    "MW": {"name": "Malawi", "currency": "MWK", "cities": ["Lilongwe", "Blantyre", "Mzuzu"]},
    "LS": {"name": "Lesotho", "currency": "LSL", "cities": ["Maseru", "Teyateyaneng", "Maputsoe"]},
    "SZ": {"name": "Eswatini", "currency": "SZL", "cities": ["Mbabane", "Manzini", "Siteki"]},
    "MG": {"name": "Madagascar", "currency": "MGA", "cities": ["Antananarivo", "Toamasina", "Fianarantsoa"]},
    "MZ": {"name": "Mozambique", "currency": "MZN", "cities": ["Maputo", "Beira", "Nampula"]},
    "MU": {"name": "Mauritius", "currency": "MUR", "cities": ["Port Louis", "Curepipe", "Quatre Bornes"]},
    "SC": {"name": "Seychelles", "currency": "SCR", "cities": ["Victoria", "Anse Boileau", "Beau Vallon"]},
    "KM": {"name": "Comoros", "currency": "KMF", "cities": ["Moroni", "Mutsamudu", "Fomboni"]},
    "NA": {"name": "Namibia", "currency": "NAD", "cities": ["Windhoek", "Swakopmund", "Walvis Bay"]}
}

# Generate 10,000 users
users = []
for i in range(1, 10001):
    country_code = random.choice(list(sadc_data.keys()))
    country_info = sadc_data[country_code]
    user = {
        "user_id": f"U{i:05d}",
        "home_country": country_code,
        "home_city": random.choice(country_info["cities"]),
        "currency": country_info["currency"]
    }
    users.append(user)

# Kafka setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_transaction():
    user = random.choice(users)
    is_fraud = random.random() < 0.05  # 5% chance of fraud

    # Location mismatch or normal
    location_country = random.choice(list(sadc_data.keys())) if is_fraud else user["home_country"]
    location_city = random.choice(sadc_data[location_country]["cities"])

    # Amount simulation
    amount = round(random.uniform(10, 5000), 2)
    if is_fraud and random.random() < 0.5:
        amount *= 10  # Inflate suspicious amount

    transaction = {
        "transaction_id": str(uuid.uuid4()),
        "user_id": user["user_id"],
        "timestamp": datetime.utcnow().isoformat(),
        "amount": amount,
        "currency": user["currency"],
        "location": {
            "country": location_country,
            "city": location_city
        },
        "is_fraud_simulated": is_fraud
    }
    return transaction

# Stream transactions
if __name__ == "__main__":
    while True:
        txn = generate_transaction()
        producer.send('transactions_raw', txn)
        print(f"Sent: {txn}")
        time.sleep(10)