import os
import time
import json
import requests
from collections import deque
from confluent_kafka import Consumer, KafkaException

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "broker:29092")
TOPIC = os.getenv("TOPIC", "test-topic")
API_URL = os.getenv("API_URL", "http://writer:5000/receive")
PERIOD = float(os.getenv("PERIOD", "5"))
GROUP_ID = os.getenv("GROUP_ID", "reader-group")
WINDOW_SIZE = int(os.getenv("WINDOW_SIZE", "10"))   # <-- NOWE

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest"
})

consumer.subscribe([TOPIC])
buffer = deque(maxlen=WINDOW_SIZE)  # <-- sliding window

print(f"[reader] Start polling topic '{TOPIC}', window size={WINDOW_SIZE}, every {PERIOD}s ...")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            time.sleep(PERIOD)
            continue
        if msg.error():
            print(f"[reader] Kafka error: {msg.error()}")
            continue

        payload_raw = msg.value().decode()
        print(f"[reader] odebrano 1 tik: {payload_raw}")

        # Kafka przysyła string z JSON - zdekoduj do obiektu
        try:
            tick = json.loads(payload_raw)
        except Exception as e:
            print(f"[reader] Błąd JSON decode: {e} => {payload_raw}")
            continue

        # ------------------------------
        # SLIDING WINDOW
        # ------------------------------
        buffer.append(tick)  # dodaj najnowszy
        print(f"[reader] okno ma: {len(buffer)}/{WINDOW_SIZE}")

        # jeśli jeszcze za małe okno — czekamy
        if len(buffer) < WINDOW_SIZE:
            continue

        # mamy pełne N → wyślij sekwencję
        sequence = list(buffer)   # kopia
        data_to_send = {"topic": TOPIC, "sequence": sequence}

        print(f"[reader] wysyłam sekwencję ({WINDOW_SIZE} tików) do API...")

        # wysyłka do REST API
        try:
            resp = requests.post(API_URL, json=data_to_send, timeout=5)
            if resp.status_code == 200:
                print(f"[reader] OK wysłano do API {API_URL}")
            else:
                print(f"[reader] Błąd API: {resp.status_code} {resp.text}")
        except Exception as e:
            print(f"[reader] Błąd wysyłki do API: {e}")

        # kolejne tiku przesuną okno automatycznie (deque maxlen)
        # kolejny poll doda nowy, najstarszy zniknie
        time.sleep(PERIOD)

except KeyboardInterrupt:
    print("[reader] Wyjście...")
finally:
    consumer.close()
