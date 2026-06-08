import os
import json
from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

kafka_producer: AIOKafkaProducer = None


async def _ensure_topic_exists():
    """
    Create Kafka topic if it doesn't exist.
    Called once at application startup before producer initializes.
    """
    admin = AIOKafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        request_timeout_ms=30000,
    )
    await admin.start()
    try:
        await admin.create_topics([
            NewTopic(
                name=KAFKA_TOPIC,
                num_partitions=1,
                replication_factor=1,
            )
        ])
        print(f"✓ Kafka topic '{KAFKA_TOPIC}' created")
    except TopicAlreadyExistsError:
        print(f"✓ Kafka topic '{KAFKA_TOPIC}' already exists")
    finally:
        await admin.close()


async def start_kafka_producer():
    """
    Ensure topic exists, then initialize and start Kafka producer.
    Called in FastAPI lifespan on startup.
    """
    global kafka_producer

    await _ensure_topic_exists()

    kafka_producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        request_timeout_ms=30000,
        retry_backoff_ms=1000,
    )
    await kafka_producer.start()
    print(f"✓ Kafka producer started on {KAFKA_BOOTSTRAP_SERVERS}")


async def stop_kafka_producer():
    """
    Gracefully stop Kafka producer.
    Called in FastAPI lifespan on shutdown.
    """
    global kafka_producer
    if kafka_producer:
        await kafka_producer.stop()
        print("✓ Kafka producer stopped")


async def publish_log(log_data: dict):
    """
    Publish event log to Kafka topic.
    """
    if not kafka_producer:
        raise RuntimeError("Kafka producer not initialized")
    await kafka_producer.send(KAFKA_TOPIC, log_data)


def get_topic_name() -> str:
    return KAFKA_TOPIC