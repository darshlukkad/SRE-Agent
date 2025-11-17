#!/usr/bin/env python3
"""
Kafka Consumer for reading user data from Users topic and storing in Redis
"""
import json
import redis
from kafka import KafkaConsumer

# Configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'Users'
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
CONSUMER_GROUP = 'sre-agent-consumers'

def main():
    """Main consumer loop"""
    print(f"Starting Kafka Consumer...")
    print(f"Topic: {TOPIC_NAME}")
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

    # Create Kafka consumer
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        auto_offset_reset='latest',  # Start from latest messages
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    print("✓ Connected to Kafka")
    print("\nWaiting for messages...\n")

    message_count = 0

    try:
        for message in consumer:
            data = message.value
            message_count += 1

            # Extract data from message
            message_id = data.get('message_id', 'UNKNOWN')
            timestamp = data.get('timestamp_utc', '')
            metrics = data.get('metrics', {})
            active_users_list = data.get('active_users_list', [])

            # Store in Redis
            try:
                # Store the full message with timestamp as key
                redis_client.set(
                    f"kafka:latest_message",
                    json.dumps(data)
                )

                # Store metrics
                redis_client.set("site:concurrent_users", json.dumps(metrics.get('active_users', 0)))
                redis_client.set("site:active_sessions", json.dumps(metrics.get('active_connections', 0)))

                # Store user list
                usernames = [user['username'] for user in active_users_list]
                redis_client.set("site:users", json.dumps(usernames))

                # Store time since online for users
                times = [60 * (message_count % 10 + 1) for _ in active_users_list]  # Mock times
                redis_client.set("site:times", json.dumps(times))

                # Store additional metrics
                redis_client.set("site:server_cpu", json.dumps(metrics.get('server_cpu_pct', 0)))
                redis_client.set("site:server_memory", json.dumps(metrics.get('server_memory_gb', 0)))
                redis_client.set("site:avg_latency", json.dumps(metrics.get('average_latency_ms', 0)))

                # Store last update timestamp
                redis_client.set("site:last_update", timestamp)

                print(f"[{message_count}] Processed: {message_id} | "
                      f"Users: {metrics.get('active_users', 0)} | "
                      f"CPU: {metrics.get('server_cpu_pct', 0)}% | "
                      f"Memory: {metrics.get('server_memory_gb', 0)}GB | "
                      f"Stored in Redis ✓")

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