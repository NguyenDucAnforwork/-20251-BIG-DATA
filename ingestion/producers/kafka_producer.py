import json, time, uuid, random, os
from kafka import KafkaProducer


producer = KafkaProducer(
bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
value_serializer=lambda v: json.dumps(v).encode("utf-8"),
acks="all",
enable_idempotence=True,
)


def gen_event():
    return {
    "event_id": str(uuid.uuid4()),
    "ts_ms": int(time.time() * 1000),
    "user_id": f"u_{random.randint(1, 10_000)}",
    "merchant_id": f"m_{random.randint(1, 500)}",
    "amount": round(random.uniform(1, 500), 2),
    "currency": "USD",
    "country": random.choice(["VN","SG","ID","MY","TH","PH","US"]),
    "device_id": f"d_{random.randint(1, 50_000)}",
    "status": random.choice(["authorized","captured","failed"]),
    "metadata": {"channel": random.choice(["app","web","pos"])},
    }


if __name__ == "__main__":
    topic = os.getenv("KAFKA_TOPIC", "payments.events")
    while True:
        evt = gen_event()
        producer.send(topic, value=evt, key=evt["event_id"].encode("utf-8"))
        time.sleep(0.05)