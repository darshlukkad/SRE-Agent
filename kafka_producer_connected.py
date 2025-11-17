#!/usr/bin/env python3
"""
Kafka Producer for generating mock Connected Device data and sending to connected_device topic
"""
import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

# Configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'connected_device'
INTERVAL_SECONDS = 1

# Sample data
SITE_IDS = ["ND-RAVEN", "ND-EAGLE", "MT-WOLF", "WY-BEAR", "CO-LYNX"]
NETWORKS = ["LTE", "5G", "LoRaWAN", "Satellite"]
INGRESS = ["MQTT-1", "MQTT-2", "CoAP-1", "HTTP-1"]
FIRMWARE_VERSIONS = ["5.0.3", "5.0.2", "5.1.0", "4.9.8"]
STATES = ["OK", "WARN", "OK", "OK"]
CODES = {
    "OK": ["GW-OK"],
    "WARN": ["BATT-LOW", "SIGNAL-WEAK", "LEAK-DETECT"]
}
MESSAGES = {
    "OK": ["All sensors online", "Operating normally", "Connected and reporting"],
    "WARN": ["Battery level low", "Signal strength degraded", "Methane leak detected"]
}

def generate_device_id():
    """Generate a random connected device ID"""
    return f"OGC-{random.randint(10000, 99999)}"

def generate_connected_device_message():
    """Generate a complete connected device message"""
    state = random.choice(STATES)
    code = random.choice(CODES[state])
    message = random.choice(MESSAGES[state])

    device_data = {
        "device_id": generate_device_id(),
        "device_type": "connected_device",
        "site_id": random.choice(SITE_IDS),
        "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "firmware": random.choice(FIRMWARE_VERSIONS),
        "metrics": {
            "wellhead_pressure_bar": round(random.uniform(120.0, 150.0), 1),
            "wellhead_temp_c": round(random.uniform(60.0, 75.0), 1),
            "flow_rate_m3_h": round(random.uniform(70.0, 100.0), 1),
            "methane_leak_ppm": round(random.uniform(0.5, 5.0), 1),
            "battery_soc_pct": round(random.uniform(75.0, 100.0), 1),
            "rssi_dbm": random.randint(-85, -60)
        },
        "status": {
            "state": state,
            "code": code,
            "message": message
        },
        "location": {
            "lat": round(random.uniform(45.0, 49.0), 4),
            "lon": round(random.uniform(-110.0, -97.0), 4)
        },
        "tags": {
            "network": random.choice(NETWORKS),
            "ingress": random.choice(INGRESS)
        }
    }

    return device_data

def main():
    """Main producer loop"""
    print(f"Starting Kafka Producer for Connected Devices...")
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
            message = generate_connected_device_message()

            # Send to Kafka
            future = producer.send(TOPIC_NAME, value=message)
            result = future.get(timeout=10)

            message_count += 1
            print(f"[{message_count}] Sent: {message['device_id']} | "
                  f"Site: {message['site_id']} | "
                  f"Pressure: {message['metrics']['wellhead_pressure_bar']} bar | "
                  f"Battery: {message['metrics']['battery_soc_pct']}% | "
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