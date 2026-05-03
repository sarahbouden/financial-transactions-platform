import json
import time
import random
import logging
from datetime import datetime, timezone
from faker import Faker
from kafka import KafkaProducer
from google.cloud import pubsub_v1
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

fake = Faker(["fr_FR", "en_US", "de_DE"])

TRANSACTION_TYPES = ["purchase", "refund", "transfer", "withdrawal", "deposit"]
CURRENCIES = ["EUR", "USD", "GBP", "CHF"]
MERCHANT_CATEGORIES = [
    "groceries", "restaurants", "transport", "healthcare",
    "entertainment", "utilities", "retail", "travel"
]
CARD_TYPES = ["VISA", "MASTERCARD", "AMEX"]
STATUSES = ["completed", "pending", "failed", "reversed"]
STATUS_WEIGHTS = [0.90, 0.05, 0.03, 0.02]


def generate_transaction() -> dict:
    amount = round(random.uniform(0.50, 5000.00), 2)
    transaction_type = random.choice(TRANSACTION_TYPES)
    if transaction_type in ("refund", "withdrawal"):
        amount = -amount
    return {
        "transaction_id": fake.uuid4(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "amount": amount,
        "currency": random.choice(CURRENCIES),
        "transaction_type": transaction_type,
        "status": random.choices(STATUSES, weights=STATUS_WEIGHTS, k=1)[0],
        "merchant_id": fake.uuid4(),
        "merchant_name": fake.company(),
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "merchant_country": fake.country_code(),
        "customer_id": fake.uuid4(),
        "customer_email": fake.email(),
        "card_type": random.choice(CARD_TYPES),
        "card_last_four": str(random.randint(1000, 9999)),
        "ip_address": fake.ipv4(),
        "device_type": random.choice(["mobile", "desktop", "tablet"]),
        "is_fraud": random.random() < 0.01,
    }


def create_kafka_producer() -> KafkaProducer:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    retries = 0
    while retries < 10:
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                compression_type="gzip",
            )
            logger.info(f"Connected to Kafka at {bootstrap}")
            return producer
        except Exception as e:
            retries += 1
            logger.warning(f"Kafka not ready (attempt {retries}/10): {e}")
            time.sleep(5)
    raise RuntimeError("Could not connect to Kafka after 10 attempts")


def create_pubsub_publisher():
    """Crée le publisher Pub/Sub."""
    project_id = os.getenv("GCP_PROJECT_ID")
    topic_id = os.getenv("PUBSUB_TOPIC", "raw-transactions")
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    logger.info(f"Pub/Sub publisher ready for topic: {topic_path}")
    return publisher, topic_path


def main():
    topic = "raw_transactions"
    batch_size = int(os.getenv("BATCH_SIZE", "100"))
    interval_seconds = int(os.getenv("INTERVAL_SECONDS", "30"))

    kafka_producer = create_kafka_producer()
    pubsub_publisher, pubsub_topic_path = create_pubsub_publisher()

    total_sent = 0
    total_pubsub_sent = 0

    while True:
        batch_start = time.time()
        pubsub_futures = []

        for _ in range(batch_size):
            tx = generate_transaction()
            tx_bytes = json.dumps(tx).encode("utf-8")

            # Envoi Kafka
            kafka_producer.send(topic, key=tx["customer_id"], value=tx)

            # Envoi Pub/Sub en parallèle (non-bloquant)
            future = pubsub_publisher.publish(
                pubsub_topic_path,
                data=tx_bytes,
                transaction_id=tx["transaction_id"],  # attribut metadata
            )
            pubsub_futures.append(future)
            total_sent += 1

        # Flush Kafka
        kafka_producer.flush()

        # Attends que tous les messages Pub/Sub soient confirmés
        for future in pubsub_futures:
            try:
                future.result(timeout=10)
                total_pubsub_sent += 1
            except Exception as e:
                logger.error(f"Pub/Sub publish failed: {e}")

        elapsed = time.time() - batch_start
        logger.info(
            f"Batch sent: {batch_size} transactions in {elapsed:.2f}s "
            f"| Kafka total: {total_sent} "
            f"| Pub/Sub total: {total_pubsub_sent}"
        )
        time.sleep(interval_seconds)


if __name__ == "__main__":
    main()