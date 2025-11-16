from kafka import KafkaProducer
from faker import Faker
import random
import json

fake = Faker()
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_user(user_id):
    gender = random.choice(["male", "female"])
    name = fake.name_male() if gender == "male" else fake.name_female()
    dob = fake.date_of_birth(minimum_age=18, maximum_age=65).isoformat()
    return {
        "user_id": f"U{user_id:05d}",
        "name": name,
        "gender": gender,
        "date_of_birth": dob
    }

if __name__ == "__main__":
    try:
        for i in range(1, 10001):
            user = generate_user(i)
            producer.send("users_profile", user)
            print(f"Sent: {user}")
    finally:
        producer.flush(timeout=1000)
        producer.close()