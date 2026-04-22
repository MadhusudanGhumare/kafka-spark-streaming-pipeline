from kafka import KafkaProducer
import json
import time
from faker import Faker
import random

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=5
)

TOPIC = "orders"

def generate_order():
    return {
        "order_id": random.randint(1000, 9999),
        "customer_name": fake.name(),
        "amount": round(random.uniform(100, 1000), 2),
        "city": fake.city(),
        "timestamp": time.time()
    }

if __name__ == "__main__":
    while True:
        data = generate_order()
        print(f"Sending: {data}")
        producer.send(TOPIC, value=data)
        time.sleep(1)
