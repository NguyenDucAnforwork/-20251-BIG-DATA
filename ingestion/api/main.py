from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Optional
from kafka import KafkaProducer
import json, os, time


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "payments.events")


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all", # idempotent producer
    # enable_idempotence=True,
    linger_ms=5,
    )


app = FastAPI(title="Ingestion API")


class PaymentEvent(BaseModel):
    event_id: str
    ts_ms: int
    user_id: str
    merchant_id: str
    amount: float
    currency: str = "USD"
    country: str | None = None
    device_id: str | None = None
    status: str = Field(..., pattern="^(authorized|captured|refunded|failed)$")
    metadata: dict | None = {}


@app.post("/ingest")
def ingest(evt: PaymentEvent):
    try:
        # simple idempotency key (event_id) can be used by downstream sink
        payload = evt.dict()
        payload["ingest_received_ms"] = int(time.time() * 1000)
        producer.send(KAFKA_TOPIC, value=payload, key=evt.event_id.encode("utf-8"))
        producer.flush()
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))