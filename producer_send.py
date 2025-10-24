import json
import time
from kafka import KafkaProducer
from random import randint, choice, uniform
import configparser

# Load INI config
config = configparser.ConfigParser()
config.read('../config.ini')

bootstrap_servers = config['Kafka']['bootstrap_servers']
topic = config['Kafka']['topic']

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = "transactions"


def generate_txn(txn_id):
    return {
        "txn_id": f"txn_{txn_id}",
        "customer_id": randint(1,5000),
        "amount": round(uniform(5.0, 2000.0), 2),
        "timestamp": int(time.time() * 1000),
        "payment_type": choice(["card","cash","upi"]),
        "store_id": choice(["S1","S2","S3"]),
        "device": choice(["pos","web","mobile"])
    }


i = 1
try:
    while True:
        msg = generate_txn(i)
        producer.send(topic, msg)
        print("sent", i, msg)
        i += 1
        time.sleep(0.5)
except KeyboardInterrupt:
    print("shutting down producer")
    producer.flush()
    producer.close()