"""
Consumes messages from the Kafka topic 'transactions_topic' 
and stores them in the database.
"""

import os
import logging
import json
import time

from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from sqlalchemy.orm import Session

from setup_db import SessionLocal
from models import Transaction, Item


# Configure logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# Load environment variables
load_dotenv()

# Kafka Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TRANSACTION_TOPIC = "transactions_topic"
BATCH_SIZE = 100  # Number of messages to process before committing


def connect_kafka():
    """Connects to Kafka with retries."""
    for attempt in range(5):
        try:
            consumer_instance = KafkaConsumer(
                TRANSACTION_TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                group_id="transactions_consumer_group",
                auto_offset_reset="earliest",  # Ensures old messages are consumed
                enable_auto_commit=True,  # Commit offsets automatically
                value_deserializer=lambda x: json.loads(x.decode("utf-8"))
            )
            logging.info("Connected to Kafka!")
            return consumer_instance
        except NoBrokersAvailable as e:
            logging.error(
                "Retrying Kafka connection (%s/5): %s", attempt + 1, e)
            time.sleep(5)
    raise NoBrokersAvailable("Kafka is not available after multiple attempts.")


def consume_transactions():
    """
    Consumes transactions from Kafka, processing one transaction at a time.
    Commits to the database in batches or every 5 minutes.
    Prints the transaction ID for each processed message.
    """
    consumer = connect_kafka()
    db_session: Session = SessionLocal()

    message_count = 0
    last_commit_time = time.time()
    commit_interval = 300  # 5 minutes

    try:
        for message in consumer:
            data = message.value

            # Validate message structure
            if not isinstance(data, dict):
                logging.error(
                    "Invalid message format: expected dict, got %s", type(data))
                continue
            if "items" not in data or not isinstance(data["items"], list):
                logging.error(
                    "Invalid 'items' field: expected list, got %s", type(data.get("items")))
                continue

            try:
                # Create transaction object
                transaction = Transaction(
                    store_id=data["store_id"],
                    timestamp=data["timestamp"],
                    total_amount=data["total_amount"]
                )

                # Add transaction to session and flush to get ID
                db_session.add(transaction)
                db_session.flush()

                transaction_id = transaction.transaction_id
                logging.info("Processing transaction ID: %s", transaction_id)

                # Process items for this transaction
                for item in data["items"]:
                    item_entry = Item(
                        transaction_id=transaction_id,
                        product_id=item["product_id"],
                        quantity=item["quantity"],
                        amount=item["amount"]
                    )
                    db_session.add(item_entry)

                message_count += 1

                # Check if batch size is reached or 5 minutes have passed
                if (message_count >= BATCH_SIZE or
                        (time.time() - last_commit_time) >= commit_interval):
                    db_session.commit()
                    logging.info(
                        "Committed %s transactions to the database.", message_count)
                    message_count = 0
                    last_commit_time = time.time()

            except KeyError as ke:
                logging.error("Missing required field in message: %s", ke)
                db_session.rollback()
            except Exception as e:
                logging.error("Error processing transaction: %s", e)
                db_session.rollback()

    except Exception as e:
        logging.error("Error consuming transactions: %s", e)
        db_session.rollback()
    finally:
        # Commit any remaining transactions
        if message_count > 0:
            try:
                db_session.commit()
                logging.info("Final commit of %s transactions.", message_count)
            except Exception as e:
                logging.error("Error in final commit: %s", e)
                db_session.rollback()

        db_session.close()
        consumer.close()
        logging.info("Consumer closed.")


if __name__ == "__main__":
    consume_transactions()
