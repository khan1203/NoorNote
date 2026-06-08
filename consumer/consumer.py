import os
import json
import asyncio
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import GroupCoordinatorNotAvailableError, KafkaConnectionError
from datetime import datetime
from dotenv import load_dotenv
from .mongodb import connect_to_mongodb, get_mongodb

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP")

MAX_RETRIES = 10
RETRY_DELAY = 5


async def consume_logs():
    """
    Kafka consumer that processes log events and saves to MongoDB.
    """
    await connect_to_mongodb()
    mongodb = get_mongodb()

    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        request_timeout_ms=30000,
        retry_backoff_ms=1000,
        session_timeout_ms=30000,
        heartbeat_interval_ms=10000,
        max_poll_interval_ms=300000,
    )

    # Startup retry loop — handles broker and metadata not yet ready
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"[{attempt}/{MAX_RETRIES}] Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
            
            # Ensure consumer instance is freshly initialized inside the try block
            if attempt > 1:
                consumer = AIOKafkaConsumer(
                    KAFKA_TOPIC,
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    group_id=KAFKA_GROUP_ID,
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                    auto_offset_reset="earliest",
                    enable_auto_commit=True,
                    request_timeout_ms=30000,
                    retry_backoff_ms=1000,
                )

            await consumer.start()
            print(f"✓ Consumer connected and subscribed to topic: {KAFKA_TOPIC}")
            break
            
        except (KafkaConnectionError, GroupCoordinatorNotAvailableError, ConnectionRefusedError, OSError) as e:
            print(f"✗ Kafka connection or topic metadata not ready ({type(e).__name__}): {e}")
            if attempt == MAX_RETRIES:
                print("Max retries reached. Exiting.")
                raise
                
            print(f"Retrying in {RETRY_DELAY}s...")
            try:
                await consumer.stop()
            except Exception:
                pass

            await asyncio.sleep(RETRY_DELAY)

    # Main consume loop
    try:
        async for message in consumer:
            event = message.value
            print(f"Received event: {event}")

            if "timestamp" in event and isinstance(event["timestamp"], str):
                event["timestamp"] = datetime.fromisoformat(event["timestamp"])

            result = await mongodb.logs.insert_one(event)
            print(f"✓ Saved to MongoDB: {result.inserted_id}")

    except Exception as e:
        print(f"Consumer loop error: {e}")
        raise

    finally:
        await consumer.stop()
        print("✓ Consumer stopped")


if __name__ == "__main__":
    print("Starting Kafka consumer...")
    asyncio.run(consume_logs())