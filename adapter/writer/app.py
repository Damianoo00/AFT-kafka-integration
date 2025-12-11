from flask import Flask, request
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic, KafkaException, KafkaError
import time
import os

app = Flask(__name__)
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "broker:29092")

admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

# opcjonalny prosty cache topiców (TTL w sekundach)
_topic_cache = {"ts": 0, "topics": set()}
TOPIC_CACHE_TTL = 5  # seconds



def delivery_report(err, msg):
    if err:
        app.logger.error(f"Błąd dostarczenia: {err}")
    else:
        app.logger.info(f"Dostarczono: {msg.topic()}:{msg.partition()}@{msg.offset()}")


def topic_exists(topic_name: str, timeout: float = 5.0) -> bool:
    now = time.time()
    if now - _topic_cache["ts"] < TOPIC_CACHE_TTL and _topic_cache["topics"]:
        return topic_name in _topic_cache["topics"]

    try:
        md = admin.list_topics(timeout=timeout)
    except Exception as e:
        app.logger.exception("Nie udało się pobrać metadata topiców: %s", e)
        return False

    topics = set(md.topics.keys())

    # update cache
    _topic_cache["ts"] = now
    _topic_cache["topics"] = topics

    tm = md.topics.get(topic_name)
    if tm is None:
        return False

    err_attr = getattr(tm, "error", None)
    if err_attr is not None:
        try:
            return err_attr.code() == 0
        except Exception:
            return not bool(err_attr)

    return True


@app.route("/write", methods=["POST"])
def write_kafka():
    obj = request.get_json()
    topic_name = obj["topic"]
    message = obj["message"]

    if not topic_exists(topic_name):
        return {"error": "Topic nie istnieje", "topic": topic_name}, 400

    try:
        producer.produce(topic_name, message.encode(), callback=delivery_report)
        # Poczekaj na wywołanie callbacków — daj dłuższy timeout jeśli trzeba
        producer.flush(10)
    except Exception as e:
        app.logger.exception("Błąd przy wysyłaniu wiadomości: %s", e)
        return {"error": "Błąd przy wysyłaniu wiadomości", "detail": str(e)}, 500

    return {"message": "ok"}


@app.route("/topic", methods=["POST"])
def create_topic_in_kafka():
    obj = request.get_json()
    topic_name = obj["topic"]

    topic = NewTopic(
        topic=topic_name,
        num_partitions=1,
        replication_factor=1
    )

    fs = admin.create_topics([topic])

    for t, f in fs.items():
        try:
            f.result()
            _topic_cache["ts"] = 0
            return {"message": f"Topic '{t}' utworzony."}
        except KafkaException as e:
            try:
                code = e.args[0].code()
            except Exception:
                code = None

            if code == KafkaError.TOPIC_ALREADY_EXISTS:
                return {"message": f"Topic '{t}' już istnieje — ignoruję."}
            else:
                return {"error": str(e)}, 400
