"""
Ride Request Producer
---------------------
Pretends to be the "Uber app". It creates fake ride requests and sends
them to Kafka, one message per ride, forever.

Each ride gets a simulated request_time spread across the day, with more
rides at morning (8-10) and evening (17-20) peaks, so the "Peak Hours"
analysis has a realistic pattern to find.
"""
import json
import os
import random
import time
import uuid
from datetime import datetime

from confluent_kafka import Producer

# ---------- Settings (can be changed with environment variables) ----------
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("TOPIC", "ride-requests")
RATE = float(os.getenv("RIDES_PER_SECOND", "2"))

# ---------- Fake world: Bengaluru zones, drivers, vehicles ----------
ZONES = [
    "Koramangala", "Indiranagar", "Whitefield", "MG Road", "Electronic City",
    "HSR Layout", "Jayanagar", "Hebbal", "Marathahalli", "Yelahanka",
]

DRIVERS = [f"D{n:03d}" for n in range(1, 51)]  # D001 ... D050
# Some drivers work much more than others (fixed seed = same pattern every run)
_rng = random.Random(42)
DRIVER_WEIGHTS = [_rng.uniform(0.3, 3.0) for _ in DRIVERS]

VEHICLES = ["Bike", "Auto", "Cab"]
VEHICLE_WEIGHTS = [3, 3, 4]
VEHICLE_PRICE_MULTIPLIER = {"Bike": 0.6, "Auto": 0.8, "Cab": 1.0}

# How busy each hour of the day is (index 0 = midnight, 23 = 11 PM)
HOUR_WEIGHTS = [1, 1, 1, 1, 1, 2, 4, 7, 10, 9, 6, 5,
                6, 5, 5, 6, 8, 10, 10, 9, 6, 4, 3, 2]
PEAK_HOURS = {8, 9, 17, 18, 19}
SURGE = 1.5

BASE_FARE = 40
PER_KM = 12


def make_ride() -> dict:
    """Build one fake ride request."""
    hour = random.choices(range(24), weights=HOUR_WEIGHTS)[0]
    request_time = datetime.now().replace(
        hour=hour, minute=random.randint(0, 59),
        second=random.randint(0, 59), microsecond=0,
    )
    pickup, drop = random.sample(ZONES, 2)
    distance_km = round(random.uniform(1.5, 25.0), 1)
    vehicle = random.choices(VEHICLES, weights=VEHICLE_WEIGHTS)[0]
    surge = SURGE if hour in PEAK_HOURS else 1.0
    status = "completed" if random.random() < 0.85 else "cancelled"

    fare = 0.0
    if status == "completed":
        fare = round((BASE_FARE + PER_KM * distance_km)
                     * VEHICLE_PRICE_MULTIPLIER[vehicle] * surge, 2)

    return {
        "ride_id": str(uuid.uuid4())[:8],
        "driver_id": random.choices(DRIVERS, weights=DRIVER_WEIGHTS)[0],
        "rider_id": f"R{random.randint(1, 2000):04d}",
        "pickup_zone": pickup,
        "drop_zone": drop,
        "vehicle_type": vehicle,
        "distance_km": distance_km,
        "surge_multiplier": surge,
        "fare": fare,
        "status": status,
        "request_time": request_time.strftime("%Y-%m-%d %H:%M:%S"),
    }


def on_delivery(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}", flush=True)


def main():
    print(f"Connecting to Kafka at {BOOTSTRAP}, topic '{TOPIC}', {RATE} rides/sec", flush=True)
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "client.id": "ride-producer"})

    sent = 0
    while True:
        ride = make_ride()
        producer.produce(
            TOPIC,
            key=ride["driver_id"],
            value=json.dumps(ride),
            callback=on_delivery,
        )
        producer.poll(0)  # lets Kafka report delivery results
        sent += 1
        if sent % 50 == 0:
            print(f"Sent {sent} rides | latest: {ride['ride_id']} "
                  f"{ride['pickup_zone']} -> {ride['drop_zone']} Rs.{ride['fare']}", flush=True)
        time.sleep(1 / RATE)


if __name__ == "__main__":
    main()
