#!/usr/bin/env python3
"""
Kafka Producer for generating mock Electrical Rotor data and sending to electrical_rotor topic
"""
import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

# Configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'electrical_rotor'
INTERVAL_SECONDS = 1

# Sample data
SITE_IDS = ["NM-SAGE", "AZ-MESA", "NV-SIERRA", "UT-ZION", "CO-PEAK"]
PANELS = ["MCC-1", "MCC-2", "MCC-3", "SWB-A", "SWB-B"]
LINES = ["R1", "R2", "R3", "R4"]
FIRMWARE_VERSIONS = ["2.4.5", "2.4.4", "2.5.0", "2.3.9"]
STATES = ["OK", "WARN", "OK", "OK"]
CODES = {
    "OK": ["ER-OK"],
    "WARN": ["TEMP-HIGH", "VIB-HIGH", "PF-LOW"]
}
MESSAGES = {
    "OK": ["Stable", "Operating normally", "All parameters nominal"],
    "WARN": ["Stator temperature elevated", "Vibration levels increasing", "Power factor below threshold"]
}

def generate_device_id():
    """Generate a random electrical rotor device ID"""
    return f"EROT-{random.randint(10000, 99999)}"

def generate_rotor_message():
    """Generate a complete electrical rotor message"""
    state = random.choice(STATES)
    code = random.choice(CODES[state])
    message = random.choice(MESSAGES[state])

    rotor_data = {
        "device_id": generate_device_id(),
        "device_type": "electrical_rotor",
        "site_id": random.choice(SITE_IDS),
        "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "firmware": random.choice(FIRMWARE_VERSIONS),
        "metrics": {
            "stator_temp_c": round(random.uniform(75.0, 95.0), 1),
            "bearing_temp_c": round(random.uniform(65.0, 82.0), 1),
            "current_a": round(random.uniform(280.0, 350.0), 1),
            "voltage_v": round(random.uniform(400.0, 440.0), 1),
            "power_factor": round(random.uniform(0.88, 0.98), 2),
            "vibration_mm_s": round(random.uniform(0.8, 2.5), 1)
        },
        "status": {
            "state": state,
            "code": code,
            "message": message
        },
        "location": {
            "lat": round(random.uniform(33.0, 41.0), 4),
            "lon": round(random.uniform(-114.0, -104.0), 4)
        },
        "tags": {
            "panel": random.choice(PANELS),
            "line": random.choice(LINES)
        }
    }

    return rotor_data

def main():
    """Main producer loop"""
    print(f"Starting Kafka Producer for Electrical Rotors...")
    print(f"Topic: {TOPIC_NAME}")
    print(f"Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Interval: {INTERVAL_SECONDS} second(s)")
    print("-" * 50)

    # Create Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=3
    )

    message_count = 0

    try:
        while True:
            # Generate message
            message = generate_rotor_message()

            # Send to Kafka
            future = producer.send(TOPIC_NAME, value=message)
            result = future.get(timeout=10)

            message_count += 1
            print(f"[{message_count}] Sent: {message['device_id']} | "
                  f"Site: {message['site_id']} | "
                  f"Current: {message['metrics']['current_a']}A | "
                  f"PF: {message['metrics']['power_factor']} | "
                  f"Status: {message['status']['state']}")

            # Wait for next interval
            time.sleep(INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("\n\nShutting down producer...")
    except Exception as e:
        print(f"\nError: {e}")
    finally:
        producer.close()
        print("Producer closed.")

if __name__ == "__main__":
    main()