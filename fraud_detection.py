import json
import random
import time
import uuid
from datetime import datetime, timezone
from confluent_kafka import Producer

conf ={
    "bootstrap.servers":"localhost:9094",
    "client.id":"fraud-aware-producer"
}

producer = Producer(conf)
TOPIC = "transactions"

IP_POOLS = {
    "US": ["34.201.10.5", "54.92.18.44", "44.42.15.24"],
    "UK": ["51.140.22.11", "35.178.90.12"],
    "PK": ["203.99.160.10", "39.32.44.7"],
    "DE": ["18.196.44.21", "3.121.98.17"]
}

CURRENCIES = ["USD", "EUR", "GBP", "PKR"]

def delivery_report(err,msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered @ offset {msg.offset()}")

def random_card():
    return "".join(str(random.randint(0, 9)) for _ in range(16))

def base_transaction(card_number, ip, currency):
    return {
        "transaction_id": f"tx-{uuid.uuid4().hex[:10]}",
        "user_id": f"user-{random.randint(1000, 9999)}",
        "card_number": card_number,
        "amount": round(random.uniform(10, 5000), 2),
        "currency": currency,
        "ip_address": ip,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

def produce_normal_transaction():
    country = random.choice(list(IP_POOLS.keys()))
    ip = random.choice(IP_POOLS[country])
    tx = base_transaction(random_card(), ip, random.choice(CURRENCIES))
    producer.produce(TOPIC, json.dumps(tx), on_delivery=delivery_report)
    
def produce_fraud_scenario():
    """
    SAME card used in different locations within 1 second
    """
    card = random_card()
    timestamp = datetime.now(timezone.utc).isoformat()

    countries = random.sample(list(IP_POOLS.keys()), 2)

    for country in countries:
        tx = {
            "transaction_id": f"tx-{uuid.uuid4().hex[:10]}",
            "user_id": f"user-{random.randint(1000, 9999)}",
            "card_number": card,
            "amount": round(random.uniform(1000, 8000), 2),
            "currency": random.choice(CURRENCIES),
            "ip_address": random.choice(IP_POOLS[country]),
            "timestamp": timestamp
        }

        producer.produce(TOPIC, json.dumps(tx), on_delivery=delivery_report)
        
def main():
    while True:
        if random.random() < 0.4:
            print("Generating FRAUD scenario")
            produce_fraud_scenario()
        else:
            produce_normal_transaction()

        producer.poll(0)
        time.sleep(random.uniform(0.3, 1.2))

if __name__ == "__main__":
    main()
