import os
import time
import requests
from confluent_kafka import Consumer, KafkaException

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "broker:29092")
TOPIC = os.getenv("TOPIC", "test-topic")
API_URL = os.getenv("API_URL", "http://writer:5000/receive")  # domyślnie wysyła do writer
PERIOD = float(os.getenv("PERIOD", "5"))  # w sekundach
GROUP_ID = os.getenv("GROUP_ID", "reader-group")

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest"
})

consumer.subscribe([TOPIC])

print(f"[reader] Start polling topic '{TOPIC}' every {PERIOD} seconds...")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            time.sleep(PERIOD)
            continue
        if msg.error():
            print(f"[reader] Kafka error: {msg.error()}")
            continue

        payload = msg.value().decode()
        print(f"[reader] Odebrano: {payload}")

        # wysyłka do REST API
        try:
            resp = requests.post(API_URL, json={"topic": TOPIC, "message": payload}, timeout=5)
            if resp.status_code == 200:
                print(f"[reader] Wysłano do API {API_URL}")
            else:
                print(f"[reader] Błąd API: {resp.status_code} {resp.text}")
        except Exception as e:
            print(f"[reader] Błąd wysyłki do API: {e}")

        time.sleep(PERIOD)
except KeyboardInterrupt:
    print("[reader] Wyjście...")
finally:
    consumer.close()
