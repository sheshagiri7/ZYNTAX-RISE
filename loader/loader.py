"""
Kafka to Neo4j Loader Daemon
============================
Consumes individual CSV row messages published by the API to Kafka,
and idempotently MERGEs them into Neo4j using the dynamic schema graph model.

Resilience:
- Implements startup retry loops for Kafka and Neo4j.
- Gracefully continues upon transient row or network errors.
- Updates JobTracker progress for GET /status honesty.
"""

import os
import sys
import json
import time
import signal
import logging
from typing import Dict, Any, Optional

# Add parent directory to sys.path to resolve backend modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_GROUP_ID,
    KAFKA_RETRY_COUNT,
    KAFKA_RETRY_DELAY,
    NEO4J_DATABASE
)
from backend.neo4j_client import get_neo4j_driver
from backend.job_tracker import tracker
from loader.neo4j_loader import merge_row_into_graph

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("zyntax.loader")

_running = True


def handle_shutdown(signum, frame):
    global _running
    logger.info("Received termination signal. Shutting down loader daemon...")
    _running = False


signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


def process_message_payload(msg_data: Dict[str, Any], driver: Optional[Any] = None) -> bool:
    """
    Processes a single message dictionary from the Kafka topic.
    Updates JobTracker with progress and completion state.
    """
    job_id = msg_data.get("job_id")
    dataset_id = msg_data.get("dataset_id")
    filename = msg_data.get("filename", "unknown.csv")
    row_index = msg_data.get("row_index", 0)
    row_data = msg_data.get("data", {})

    if not dataset_id or row_index <= 0:
        logger.warning(f"Invalid message received: {msg_data}")
        if job_id:
            tracker.update_progress(job_id, failed_inc=1)
        return False

    if driver is None:
        driver = get_neo4j_driver(retries=1)

    # Perform MERGE into Neo4j
    success = merge_row_into_graph(
        driver=driver,
        dataset_id=dataset_id,
        filename=filename,
        row_index=row_index,
        row_data=row_data,
        database=NEO4J_DATABASE
    )

    if job_id:
        if success:
            tracker.update_progress(job_id, loaded_inc=1)
        else:
            tracker.update_progress(job_id, failed_inc=1)

    return success


def run_loader_loop():
    """
    Main loop: connects to Kafka consumer and processes row messages continuously.
    """
    from kafka import KafkaConsumer

    logger.info(f"Starting ZYNTAX Loader daemon. Listening on topic '{KAFKA_TOPIC}'...")

    consumer = None
    for attempt in range(1, KAFKA_RETRY_COUNT + 1):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                group_id=KAFKA_GROUP_ID,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=1000
            )
            logger.info("Connected to Kafka consumer.")
            break
        except Exception as e:
            logger.warning(f"Kafka consumer connection attempt {attempt} failed: {e}")
            if attempt < KAFKA_RETRY_COUNT:
                time.sleep(KAFKA_RETRY_DELAY)

    if consumer is None:
        logger.error("Failed to connect to Kafka. Exiting loader daemon.")
        return

    # Obtain Neo4j driver
    driver = get_neo4j_driver(retries=10, delay=2.0)

    logger.info("Loader initialized and ready to consume messages.")
    while _running:
        try:
            for message in consumer:
                if not _running:
                    break
                try:
                    msg_data = message.value
                    process_message_payload(msg_data, driver=driver)
                except Exception as e:
                    logger.error(f"Error processing record from Kafka: {e}")
        except Exception as e:
            if _running:
                logger.warning(f"Kafka polling error: {e}. Retrying in 2 seconds...")
                time.sleep(2.0)

    if consumer:
        consumer.close()
    logger.info("Loader daemon stopped cleanly.")


if __name__ == "__main__":
    run_loader_loop()
