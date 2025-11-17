#!/usr/bin/env python3
"""
Kafka Producer for generating mock Turbine data and sending to Turbines topic
"""
import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

# Configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'Turbines'
INTERVAL_SECONDS = 1

# Sample data for generating realistic mock data
SITE_IDS = ["WY-ALPHA", "WY-BETA", "TX-GAMMA", "ND-DELTA", "NM-EPSILON"]
VENDORS = ["HanTech", "TurboSys", "PowerGen", "MegaFlow"]
LOOPS = ["A1", "A2", "B1", "B2", "C1"]
FIRMWARE_VERSIONS = ["3.2.1", "3.2.0", "3.1.9", "3.3.0"]
STATES = ["OK", "WARN", "OK", "OK"]  # Weighted towards OK
CODES = {
    "OK": ["TURB-OK"],
    "WARN": ["TURB-WARN-TEMP", "TURB-WARN-VIB", "TURB-WARN-PRESS"]
}
MESSAGES = {
    "OK": ["Nominal", "Operating normally", "All parameters within range"],
    "WARN": ["Inlet temperature slightly elevated", "Vibration trending up", "Pressure variation detected"]
}

def generate_device_id():
    """Generate a random turbine device ID"""
    return f"TURB-{random.randint(10000, 99999)}"

def generate_turbine_message():
    """Generate a complete turbine message"""
    state = random.choice(STATES)
    code = random.choice(CODES[state])
    message = random.choice(MESSAGES[state])

    turbine_data = {
        "device_id": generate_device_id(),
        "device_type": "turbine",
        "site_id": random.choice(SITE_IDS),
        "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "firmware": random.choice(FIRMWARE_VERSIONS),
        "metrics": {
            "rpm": random.randint(3200, 3800),
            "inlet_temp_c": round(random.uniform(380.0, 450.0), 1),
            "exhaust_temp_c": round(random.uniform(500.0, 580.0), 1),
            "vibration_mm_s": round(random.uniform(1.5, 4.0), 1),
            "pressure_bar": round(random.uniform(15.0, 20.0), 1),
            "power_kw": round(random.uniform(11000.0, 14000.0), 1),
            "fuel_flow_kg_h": round(random.uniform(380.0, 480.0), 1),
            "no_x_ppm": round(random.uniform(25.0, 45.0), 1)
        },
        "status": {
            "state": state,
            "code": code,
            "message": message
        },
        "location": {
            "lat": round(random.uniform(30.0, 48.0), 4),
            "lon": round(random.uniform(-110.0, -95.0), 4)
        },
        "tags": {
            "vendor": random.choice(VENDORS),
            "loop": random.choice(LOOPS)
        }
    }

    return turbine_data

def main():
    """Main producer loop"""
    print(f"Starting Kafka Producer for Turbines...")
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
            message = generate_turbine_message()

            # Send to Kafka
            future = producer.send(TOPIC_NAME, value=message)
            result = future.get(timeout=10)

            message_count += 1
            print(f"[{message_count}] Sent: {message['device_id']} | "
                  f"Site: {message['site_id']} | "
                  f"RPM: {message['metrics']['rpm']} | "
                  f"Power: {message['metrics']['power_kw']} kW | "
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