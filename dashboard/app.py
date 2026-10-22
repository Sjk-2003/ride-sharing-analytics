"""
SwiftRide Bengaluru - Live Operations Center (backend)
------------------------------------------------------
A background thread reads every ride from the Kafka topic and updates running
totals (see stats.py). FastAPI serves those totals as JSON and serves the
website from ./static.

On every start the thread reads the topic from the very beginning (a new random
consumer group, offsets never saved), so the numbers are rebuilt from whatever
Kafka still holds, then it keeps reading new rides live.
"""
import json
import logging
import os
import threading
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, KafkaException
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from stats import RideStats

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TOPIC", "ride-requests")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("dashboard")

stats = RideStats()
health = {
    "kafka_connected": False,
    "topic_found": False,
    "messages_consumed": 0,
    "last_error": None,
    "started_at": time.time(),
}


def _on_assign(consumer, partitions):
    # Always start from the first message Kafka still has, and count from zero
    # (so a re-assignment never counts the same ride twice).
    for p in partitions:
        p.offset = OFFSET_BEGINNING
    consumer.assign(partitions)
    # Count it as a "topic reset" only if we had already read rides (e.g. Kafka restarted)
    stats.reset(count=stats.last_offset >= 0)
    log.info("Assigned %s - reading from the beginning", [p.partition for p in partitions])


def _check_kafka(consumer):
    """Ask Kafka for its topic list: tells us if it is reachable and if our topic exists."""
    try:
        md = consumer.list_topics(timeout=5)
        health["kafka_connected"] = True
        health["topic_found"] = TOPIC in md.topics and md.topics[TOPIC].error is None
        if health["topic_found"]:
            health["last_error"] = None      # all good again: forget old errors
    except KafkaException as e:
        health["kafka_connected"] = False
        health["last_error"] = str(e)


def consume_forever():
    """Runs in a background thread. Never exits: any error -> wait and reconnect."""
    while True:
        consumer = None
        try:
            consumer = Consumer({
                "bootstrap.servers": BOOTSTRAP,
                "group.id": f"swiftride-dashboard-{uuid.uuid4().hex[:8]}",
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
                "client.id": "swiftride-dashboard",
                "socket.keepalive.enable": True,
            })
            consumer.subscribe([TOPIC], on_assign=_on_assign)
            log.info("Consumer started for %s on %s", TOPIC, BOOTSTRAP)
            last_check = 0.0
            while True:
                if time.time() - last_check > 5:
                    _check_kafka(consumer)
                    last_check = time.time()
                for msg in consumer.consume(num_messages=500, timeout=1.0):
                    err = msg.error()
                    if err:
                        if err.code() != KafkaError._PARTITION_EOF:
                            health["last_error"] = str(err)
                            if err.code() in (KafkaError._TRANSPORT, KafkaError._ALL_BROKERS_DOWN):
                                health["kafka_connected"] = False
                        continue
                    ts_type, ts_ms = msg.timestamp()
                    arrival = ts_ms / 1000.0 if ts_ms and ts_ms > 0 else time.time()
                    try:
                        ride = json.loads(msg.value())
                    except (TypeError, ValueError):
                        ride = None
                    stats.add(ride if isinstance(ride, dict) else {}, arrival, msg.offset())
                    health["messages_consumed"] += 1
                    health["kafka_connected"] = True
        except Exception as e:  # never crash the website because of Kafka
            health["kafka_connected"] = False
            health["last_error"] = str(e)
            log.warning("Kafka consumer problem (%s) - retrying in 5 s", e)
            time.sleep(5)
        finally:
            if consumer is not None:
                try:
                    consumer.close()
                except Exception:
                    pass


@asynccontextmanager
async def lifespan(_app):
    threading.Thread(target=consume_forever, name="kafka-consumer", daemon=True).start()
    yield


app = FastAPI(title="SwiftRide Bengaluru - Live Operations Center", lifespan=lifespan)


def _health(snap):
    s = snap["summary"]
    return {
        "status": "ok",                              # the website itself is up
        "kafka_bootstrap": BOOTSTRAP,
        "topic": TOPIC,
        "kafka_connected": health["kafka_connected"],
        "topic_found": health["topic_found"],
        "messages_consumed": health["messages_consumed"],
        "rides_in_totals": s["total_requests"],
        "last_message_age_s": s["last_message_age_s"],
        "rides_last_minute": s["rides_last_minute"],
        "topic_resets_seen": snap["reset_count"],
        "bad_messages": snap["bad_messages"],
        "last_error": health["last_error"],
        "uptime_s": round(time.time() - health["started_at"]),
    }


@app.get("/api/all")
def api_all():
    snap = stats.snapshot()
    snap["health"] = _health(snap)
    return snap


@app.get("/api/health")
def api_health():
    return _health(stats.snapshot())


@app.get("/api/summary")
def api_summary():
    return stats.snapshot()["summary"]


@app.get("/api/peak-hours")
def api_peak_hours():
    return stats.snapshot()["peak_hours"]


@app.get("/api/drivers")
def api_drivers():
    return stats.snapshot()["drivers"]


@app.get("/api/zones")
def api_zones():
    return stats.snapshot()["zones"]


@app.get("/api/vehicles")
def api_vehicles():
    snap = stats.snapshot()
    return {"vehicles": snap["vehicles"], "pricing": snap["pricing"]}


@app.get("/api/recent")
def api_recent():
    return stats.snapshot()["recent"]


# The website (index.html, style.css, app.js) - mounted last so /api/* wins
app.mount("/", StaticFiles(directory=Path(__file__).parent / "static", html=True), name="static")
