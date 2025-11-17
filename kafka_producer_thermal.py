#!/usr/bin/env python3
"""
Kafka Producer for generating mock Thermal Engine data and sending to thermal_engine topic
"""
import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

# Configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'thermal_engine'
INTERVAL_SECONDS = 1

# Sample data
SITE_IDS = ["TX-EAGLE", "TX-FALCON", "OK-HAWK", "LA-CONDOR", "NM-RAVEN"]
SKIDS = ["TE-01", "TE-02", "TE-03", "TE-04", "TE-05", "TE-06", "TE-07", "TE-08"]
PHASES = ["commissioned", "testing", "operational", "maintenance"]
FIRMWARE_VERSIONS = ["1.9.0", "1.8.5", "2.0.0", "1.9.2"]
STATES = ["OK", "WARN", "OK", "OK"]
CODES = {
    "OK": ["THRM-OK"],
    "WARN": ["OIL-LOW", "TEMP-HIGH", "SOOT-HIGH"]
}
MESSAGES = {
    "OK": ["Nominal operation", "All systems normal", "Operating within parameters"],
    "WARN": ["Oil pressure trending low", "Coolant temperature elevated", "Soot percentage increasing"]
}

def generate_device_id():
    """Generate a random thermal engine device ID"""
    return f"THRM-{random.randint(10000, 99999)}"

def generate_thermal_message():
    """Generate a complete thermal engine message"""
    state = random.choice(STATES)
    code = random.choice(CODES[state])
    message = random.choice(MESSAGES[state])

    thermal_data = {
        "device_id": generate_device_id(),
        "device_type": "thermal_engine",
        "site_id": random.choice(SITE_IDS),
        "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "firmware": random.choice(FIRMWARE_VERSIONS),
        "metrics": {
            "rpm": random.randint(1600, 2100),
            "coolant_temp_c": round(random.uniform(85.0, 98.0), 1),
            "oil_temp_c": round(random.uniform(92.0, 105.0), 1),
            "oil_pressure_bar": round(random.uniform(3.8, 5.2), 1),
            "load_pct": round(random.uniform(60.0, 95.0), 1),
            "fuel_rate_l_h": round(random.uniform(120.0, 200.0), 1),
            "soot_pct": round(random.uniform(0.3, 1.2), 1)
        },
        "status": {
            "state": state,
            "code": code,
            "message": message
        },
        "location": {
            "lat": round(random.uniform(29.0, 36.0), 4),
            "lon": round(random.uniform(-106.0, -94.0), 4)
        },
        "tags": {
            "skid": random.choice(SKIDS),
            "phase": random.choice(PHASES)
        }
    }

    return thermal_data

def main():
    """Main producer loop"""
    print(f"Starting Kafka Producer for Thermal Engines...")
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
            message = generate_thermal_message()

            # Send to Kafka
            future = producer.send(TOPIC_NAME, value=message)
            result = future.get(timeout=10)

            message_count += 1
            print(f"[{message_count}] Sent: {message['device_id']} | "
                  f"Site: {message['site_id']} | "
                  f"RPM: {message['metrics']['rpm']} | "
                  f"Load: {message['metrics']['load_pct']}% | "
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