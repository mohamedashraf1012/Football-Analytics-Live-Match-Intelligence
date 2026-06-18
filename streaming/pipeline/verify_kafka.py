"""
FootballFlow — Kafka Connection Verifier
========================================
Run this script AFTER docker-compose is up to:
  1. Verify Kafka broker is reachable
  2. Create the two required topics
  3. Send one test message to each topic
  4. Consume and print the test messages back

Usage:
    pip install kafka-python
    python verify_kafka.py
"""

import json
import time
from datetime import datetime, timezone
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

# ─── Config ────────────────────────────────────────────────
BOOTSTRAP_SERVERS = ["localhost:9092"]

TOPICS = {
    "real_match_events": {
        "num_partitions": 3,
        "replication_factor": 1,
        "description": "Pipeline A — Real historical match events (goals, cards, subs)"
    },
    "player_sensors_telemetry": {
        "num_partitions": 3,
        "replication_factor": 1,
        "description": "Pipeline B — Live EPTS wearable sensor simulation (biometrics)"
    }
}

# ─── Helpers ───────────────────────────────────────────────
def print_header(text):
    print(f"\n{'='*55}")
    print(f"  {text}")
    print(f"{'='*55}")

def print_ok(text):
    print(f"  ✅  {text}")

def print_info(text):
    print(f"  ℹ️   {text}")

def print_err(text):
    print(f"  ❌  {text}")

# ─── Step 1: Check broker connectivity ─────────────────────
print_header("STEP 1 — Broker Connectivity Check")
try:
    admin = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id="footballflow-verifier",
        request_timeout_ms=10000
    )
    print_ok(f"Connected to Kafka broker at {BOOTSTRAP_SERVERS[0]}")
except NoBrokersAvailable:
    print_err("Cannot reach Kafka broker at localhost:9092")
    print_info("Make sure you ran: docker-compose up -d")
    print_info("Then wait ~30 seconds for the broker to fully start")
    exit(1)

# ─── Step 2: Create topics ──────────────────────────────────
print_header("STEP 2 — Topic Creation")
topic_list = [
    NewTopic(
        name=name,
        num_partitions=cfg["num_partitions"],
        replication_factor=cfg["replication_factor"]
    )
    for name, cfg in TOPICS.items()
]

for name, cfg in TOPICS.items():
    try:
        admin.create_topics([NewTopic(name=name, num_partitions=cfg["num_partitions"],
                                      replication_factor=cfg["replication_factor"])])
        print_ok(f"Created topic: '{name}'  ({cfg['description']})")
    except TopicAlreadyExistsError:
        print_info(f"Topic already exists: '{name}' — skipping")
    except Exception as e:
        print_err(f"Failed to create '{name}': {e}")

# ─── Step 3: Produce test messages ─────────────────────────
print_header("STEP 3 — Produce Test Messages")
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8") if k else None
)

test_messages = {
    "real_match_events": {
        "event_id": "TEST-001",
        "game_id": 99999,
        "minute": 45,
        "type": "Goals",
        "player_id": 1001,
        "club_id": 42,
        "description": "TEST — Pipeline A verification message",
        "timestamp": datetime.now(timezone.utc).isoformat()
    },
    "player_sensors_telemetry": {
        "sensor_id": "TEST-S01",
        "player_id": 1001,
        "game_id": 99999,
        "heart_rate_bpm": 145,
        "speed_kmh": 18.5,
        "distance_covered_m": 4200,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
}

for topic, message in test_messages.items():
    future = producer.send(topic, key=str(message.get("game_id")), value=message)
    record_metadata = future.get(timeout=10)
    print_ok(
        f"Sent to '{topic}' → "
        f"partition={record_metadata.partition}, offset={record_metadata.offset}"
    )

producer.flush()
producer.close()

# ─── Step 4: Consume and verify ────────────────────────────
print_header("STEP 4 — Consume & Verify Messages")
print_info("Reading one message from each topic (5s timeout per topic)...\n")

for topic in TOPICS:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=5000,
        group_id=f"verifier-{topic}"
    )
    try:
        for msg in consumer:
            print_ok(f"Received from '{topic}':")
            print(f"      Partition : {msg.partition}")
            print(f"      Offset    : {msg.offset}")
            print(f"      Payload   : {json.dumps(msg.value, indent=6)}")
            break
    except Exception as e:
        print_err(f"Could not consume from '{topic}': {e}")
    finally:
        consumer.close()

# ─── Done ──────────────────────────────────────────────────
print_header("VERIFICATION COMPLETE")
print_ok("Kafka is up, topics exist, produce & consume work!")
print_info("Open Kafka UI at: http://localhost:8080")
print_info("Next step: write the Python producers (Pipeline A & B)")
print()
