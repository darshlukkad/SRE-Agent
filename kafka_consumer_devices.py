#!/usr/bin/env python3
"""
Kafka Consumer for reading device data from multiple topics and storing in Redis
Topics: Turbines, thermal_engine, connected_device, electrical_rotor
"""
import json
import redis
from kafka import KafkaConsumer
from collections import deque

# Configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
TOPICS = ['Turbines', 'thermal_engine', 'connected_device', 'electrical_rotor']
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
CONSUMER_GROUP = 'device-consumers'
MAX_DEVICE_LIST_SIZE = 10

# Track unique device IDs and concurrent devices
device_tracker = {}  # {device_id: count}
unique_sensors = set()  # Set of unique device_ids seen
device_list = deque(maxlen=MAX_DEVICE_LIST_SIZE)  # Last 10 device names

def main():
    """Main consumer loop"""
    print(f"Starting Kafka Consumer for Devices...")
    print(f"Topics: {TOPICS}")
    print(f"Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Consumer Group: {CONSUMER_GROUP}")
    print(f"Redis: {REDIS_HOST}:{REDIS_PORT}")
    print("-" * 50)

    # Connect to Redis
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=0,
        decode_responses=True
    )

    # Test Redis connection
    try:
        redis_client.ping()
        print("✓ Connected to Redis")
    except Exception as e:
        print(f"✗ Failed to connect to Redis: {e}")
        return

    # Create Kafka consumer for multiple topics
    consumer = KafkaConsumer(
        *TOPICS,  # Subscribe to all device topics
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    print("✓ Connected to Kafka")
    print(f"✓ Subscribed to topics: {', '.join(TOPICS)}")
    print("\nWaiting for messages...\n")

    message_count = 0

    try:
        for message in consumer:
            data = message.value
            message_count += 1

            # Extract data from message
            device_id = data.get('device_id', 'UNKNOWN')
            device_type = data.get('device_type', 'unknown')
            site_id = data.get('site_id', '')
            timestamp = data.get('timestamp_utc', '')
            status = data.get('status', {}).get('state', 'UNKNOWN')

            # Track device occurrences
            if device_id not in device_tracker:
                device_tracker[device_id] = 0
            device_tracker[device_id] += 1

            # Track unique sensors (new device_id = new sensor)
            unique_sensors.add(device_id)

            # Add to device list (keep last 10)
            device_name = f"{device_type}_{device_id}"
            if device_name not in list(device_list):
                device_list.append(device_name)

            # Calculate metrics
            concurrent_devices = len(device_tracker)  # Total unique device IDs
            active_sensors = len(unique_sensors)  # Total unique sensors seen

            # Store in Redis
            try:
                # Store device metrics
                redis_client.set("site:concurrent_devices", json.dumps(concurrent_devices))
                redis_client.set("site:active_sensors", json.dumps(active_sensors))

                # Store latest 10 device names
                redis_client.set("site:devices", json.dumps(list(device_list)))

                # Store device times (mock - using count as proxy)
                device_times = [device_tracker.get(dev.split('_', 1)[1], 0) * 60 for dev in device_list]
                redis_client.set("site:device_times", json.dumps(device_times))

                # Store device status
                redis_client.set("site:device_status", json.dumps(status))

                # Store last update timestamp
                redis_client.set("site:device_last_update", timestamp)

                # Store full device data for debugging
                redis_client.set(f"device:{device_id}", json.dumps(data))

                print(f"[{message_count}] Processed: {device_id} ({device_type}) | "
                      f"Site: {site_id} | "
                      f"Status: {status} | "
                      f"Concurrent: {concurrent_devices} | "
                      f"Total Sensors: {active_sensors}")

            except Exception as e:
                print(f"[{message_count}] Error storing to Redis: {e}")

    except KeyboardInterrupt:
        print("\n\nShutting down consumer...")
    except Exception as e:
        print(f"\nError: {e}")
    finally:
        consumer.close()
        redis_client.close()
        print("Consumer closed.")

if __name__ == "__main__":
    main()