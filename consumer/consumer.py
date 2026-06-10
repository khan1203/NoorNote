import os
import json
import asyncio
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import GroupCoordinatorNotAvailableError, KafkaConnectionError
from datetime import datetime
from dotenv import load_dotenv
from .mongodb import connect_to_consumer_mongodb, get_mongodb

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP")

MAX_RETRIES = 10
RETRY_DELAY = 5


def make_consumer():
    return AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        request_timeout_ms=30000,
        retry_backoff_ms=1000,
        session_timeout_ms=30000,
        heartbeat_interval_ms=10000,
        max_poll_interval_ms=300000,
    )


async def consume_logs():
    """
    Kafka consumer that processes log events and saves to MongoDB.
    Restarts automatically on coordinator loss or any runtime error.
    """
    while True:
        await connect_to_consumer_mongodb()
        mongodb = get_mongodb()

        consumer = make_consumer()

        # Startup retry loop
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                print(f"[{attempt}/{MAX_RETRIES}] Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
                await consumer.start()
                print(f"✓ Consumer connected and subscribed to topic: {KAFKA_TOPIC}")
                break
            except (KafkaConnectionError, GroupCoordinatorNotAvailableError, ConnectionRefusedError, OSError) as e:
                print(f"✗ Kafka not ready ({type(e).__name__}): {e}")
                try:
                    await consumer.stop()
                except Exception:
                    pass
                if attempt == MAX_RETRIES:
                    print("Max retries reached. Exiting.")
                    raise
                print(f"Retrying in {RETRY_DELAY}s...")
                await asyncio.sleep(RETRY_DELAY)

        # Main consume loop
        try:
            async for message in consumer:
                event = message.value
                print(f"Received event: {event}")

                if "timestamp" in event and isinstance(event["timestamp"], str):
                    event["timestamp"] = datetime.fromisoformat(event["timestamp"])

                result = await mongodb.activity_logs.insert_one(event)
                print(f"✓ Saved to MongoDB: {result.inserted_id}")

                await consumer.commit()

        except Exception as e:
            print(f"Consumer loop error ({type(e).__name__}): {e}")
            print(f"Restarting consumer in {RETRY_DELAY}s...")

        finally:
            try:
                await consumer.stop()
                print("✓ Consumer stopped, restarting...")
            except Exception:
                pass

        await asyncio.sleep(RETRY_DELAY)


if __name__ == "__main__":
    print("Starting Kafka consumer...")
    asyncio.run(consume_logs())