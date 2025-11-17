#!/usr/bin/env python3
"""
Kafka Producer for generating mock user data and sending to Users topic
"""
import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

# Configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'Users'
INTERVAL_SECONDS = 2
MAX_ACTIVE_USERS = 400

# Sample data for generating realistic mock data
FIRST_NAMES = ["alex", "maria", "chen", "sarah", "john", "priya", "ahmed", "emma", "david", "yuki",
               "carlos", "lisa", "raj", "anna", "michael", "sofia", "james", "mei", "robert", "nina"]
LAST_NAMES = ["j", "k", "li", "smith", "brown", "patel", "khan", "wilson", "jones", "tanaka",
              "garcia", "miller", "kumar", "davis", "rodriguez", "chen", "martinez", "anderson", "taylor", "thomas"]
REGIONS = ["US-WEST", "US-EAST", "EU-WEST", "EU-CENTRAL", "APAC", "LATAM", "MEA"]
CONNECTION_STATUSES = ["active", "idle", "active", "active"]  # Weighted towards active

def generate_user_id():
    """Generate a random user ID"""
    return f"USR-{random.randint(10000, 99999)}"

def generate_session_id():
    """Generate a random session ID"""
    chars = "ABCDEF0123456789"
    return f"SESS-{''.join(random.choices(chars, k=6))}"

def generate_ip_address():
    """Generate a random IP address"""
    ip_types = ["192.168", "10.10", "172.16"]
    base = random.choice(ip_types)
    return f"{base}.{random.randint(1, 255)}.{random.randint(1, 255)}"

def generate_username():
    """Generate a random username"""
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    return f"{first}_{last}"

def generate_user_entry():
    """Generate a single user entry"""
    # Random login time within last 30 minutes
    from datetime import timedelta
    minutes_ago = random.randint(1, 30)
    login_time = datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)

    return {
        "user_id": generate_user_id(),
        "username": generate_username(),
        "session_id": generate_session_id(),
        "login_time": login_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "ip_address": generate_ip_address(),
        "region": random.choice(REGIONS),
        "connection_status": random.choice(CONNECTION_STATUSES)
    }

def generate_message():
    """Generate a complete message with all required fields"""
    # Generate random number of active users (up to MAX_ACTIVE_USERS)
    num_active_users = random.randint(50, MAX_ACTIVE_USERS)
    num_connections = int(num_active_users * random.uniform(0.85, 0.95))

    # Generate active users list
    active_users_list = [generate_user_entry() for _ in range(min(num_active_users, 10))]  # Limit to 10 in list for message size

    # Generate message ID
    date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
    msg_id = f"MSG-{date_str}-{random.randint(10000, 99999)}"

    message = {
        "message_id": msg_id,
        "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "site_id": "SFO-WEB-01",
        "metrics": {
            "active_users": num_active_users,
            "active_connections": num_connections,
            "server_cpu_pct": round(random.uniform(30.0, 95.0), 1),
            "server_memory_gb": round(random.uniform(10.0, 28.0), 1),
            "average_latency_ms": round(random.uniform(20.0, 150.0), 1)
        },
        "active_users_list": active_users_list,
        "queue_metadata": {
            "topic": "webapp/active_users",
            "producer": "SiteSimEngine",
            "priority": "normal",
            "retries": 0
        }
    }

    return message

def main():
    """Main producer loop"""
    print(f"Starting Kafka Producer...")
    print(f"Topic: {TOPIC_NAME}")
    print(f"Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Interval: {INTERVAL_SECONDS} seconds")
    print(f"Max Active Users: {MAX_ACTIVE_USERS}")
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
            message = generate_message()

            # Send to Kafka
            future = producer.send(TOPIC_NAME, value=message)
            result = future.get(timeout=10)

            message_count += 1
            print(f"[{message_count}] Sent message: {message['message_id']} | "
                  f"Active Users: {message['metrics']['active_users']} | "
                  f"CPU: {message['metrics']['server_cpu_pct']}% | "
                  f"Latency: {message['metrics']['average_latency_ms']}ms")

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