import os
import json
import time
import requests
from collections import defaultdict, deque
from confluent_kafka import Consumer, KafkaException

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "broker:29092")
TOPIC = os.getenv("TOPIC", "test-topic")
API_URL = os.getenv("API_URL", "http://writer:5000/receive")
GROUP_ID = os.getenv("GROUP_ID", "reader-group")
WINDOW_SIZE = int(os.getenv("WINDOW_SIZE", "10"))
CLEAR_DEQUE = bool(os.getenv("CLEAR_DEQUE", "0"))

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest"
})

consumer.subscribe([TOPIC])

# 🔴 bufory per session_id
session_windows = defaultdict(lambda: deque(maxlen=WINDOW_SIZE))

print(f"[reader] Start polling topic '{TOPIC}', window size={WINDOW_SIZE} ...")

try:
    while True:
        msg = consumer.poll(timeout=0.1)
        if msg is None:
            continue
        if msg.error():
            print(f"[reader] Kafka error: {msg.error()}")
            continue

        raw = msg.value().decode()
        print(f"[reader] tick: {raw}")

        try:
            tick = json.loads(raw)
        except Exception as e:
            print(f"[reader] JSON decode error: {e} => {raw}")
            continue

        # ------------------------------
        # WYCIĄGNIJ session_id I groupuj
        # ------------------------------
        session_id = tick.get("session_id")
        if not session_id:
            print("[reader] brak session_id → ignoruję")
            continue

        request_time = tick.get("request_time")
        if not request_time:
            print("[reader] brak request_time → ignoruję")
            continue

        session_windows[session_id].append(tick)
        win = session_windows[session_id]
        print(f"[reader] session {session_id} → {len(win)}/{WINDOW_SIZE}")

        # jeśli za mało elementów → czekamy
        if len(win) < WINDOW_SIZE:
            continue

        # pełne okno → przygotuj payload
        sequence = list(win)  # kopia

        # 🔴 nowy format:
        payload = {
            "messages": [msg for t in sequence for msg in t["messages"]],
            "session_id": session_id,
            "request_time": request_time,
        }

        print(f"[reader] wysyłam pełne okno session {session_id} → {WINDOW_SIZE} tików")

        try:
            resp = requests.post(API_URL, json=payload, timeout=5)
            if resp.status_code == 200:
                print(f"[reader] OK wysłano do API {API_URL}")
            else:
                print(f"[reader] API error: {resp.status_code} {resp.text}")
        except Exception as e:
            print(f"[reader] wysyłka error: {e}")

        # przesuwamy okno normalnie, chyba że CLEAR_DEQUE=1
        if CLEAR_DEQUE:
            print(f"[reader] CLEAR_DEQUE=1 → czyszczę okno session {session_id}")
            win.clear()

        # kolejne ticki automatycznie przesuwają okno (deque maxlen)
        # nic nie czyścimy — następne batch-e będą wysyłane gdy okno "przesunie się"

except KeyboardInterrupt:
    print("[reader] wyjście...")
finally:
    consumer.close()
