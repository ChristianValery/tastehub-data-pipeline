"""
This script generates transactions in real-time and sends them to Kafka.

It also inserts historical transactions into the database.

The transactions are generated based on products, stores, and probabilities.

The transactions are sent to the Kafka topic 'transactions_topic'.

The historical transactions are inserted into the 'transactions' and 'items' tables in the database.
"""


import os
import random
from time import sleep
from datetime import datetime
import json
import logging
from typing import List, Tuple

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from transaction_generator import (
    Product,
    Store,
    TransactionSimulator)


# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()

# Kafka Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
TRANSACTION_TOPIC = 'transactions_topic'
DATABASE_URL = os.getenv('DATABASE_URL')
BATCH_SIZE = 100


# Initialize database connection
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def load_products() -> List[Product]:
    """
    Load products information from the database.
    """
    try:
        products_df = pd.read_sql_table('products', engine)
        return [
            Product(row['product_id'], row['name'],
                    row['price'], row['category'])
            for _, row in products_df.iterrows()
        ]
    except SQLAlchemyError as e:
        logging.error("Failed to read products from database: %s", e)
        return []


def load_stores() -> Tuple[List[Store], List[float]]:
    """
    Load stores information from the database.
    """
    try:
        stores_df = pd.read_sql_table('stores', engine)
        stores_list = [
            Store(row['store_id'], row['name'], row['city'])
            for _, row in stores_df.iterrows()
        ]
        stores_df["probability"] = stores_df["population"] / \
            stores_df["population"].sum()
        return stores_list, stores_df["probability"].tolist()
    except SQLAlchemyError as e:
        logging.error("Failed to read stores from database: %s", e)
        return [], []


def connect_kafka(retries: int = 5) -> KafkaProducer:
    """
    Initialize KafkaProducer with retries.
    """
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logging.info("Connected to Kafka at %s", KAFKA_BROKER)
            return producer
        except NoBrokersAvailable as e:
            logging.error(
                "Attempt %s: Failed to connect to Kafka: %s", attempt + 1, e)
            sleep(5)
    raise NoBrokersAvailable("Failed to connect to Kafka after 5 attempts")


def produce_transactions(
        simulator: TransactionSimulator,
        producer: KafkaProducer):
    """
    Generates and sends transactions to Kafka one at a time.
    """
    while True:
        now = datetime.now()
        base_prob, _ = simulator.get_transaction_probability(now)

        if base_prob == 0.0:
            sleep(60)
            continue

        transaction = simulator.generate_transaction()
        if transaction:
            # Convert Transaction object to a dictionary, ensuring 'items' are also dictionaries
            transaction_dict = {
                "store_id": transaction.store_id,
                "timestamp": transaction.timestamp,
                "total_amount": round(transaction.total_amount, 2),
                "items": [
                    {
                        "product_id": item.product_id,
                        "quantity": item.quantity,
                        "amount": round(item.amount, 2)
                    } for item in transaction.items
                ]
            }

            producer.send(TRANSACTION_TOPIC, value=transaction_dict)
            logging.info("Sent transaction to Kafka: %s", transaction_dict)

        sleep_time = max(1, int(random.expovariate(base_prob)))
        sleep(sleep_time)


def main():
    """
    Main function to generate transactions and send them to Kafka.
    """
    # Load products and stores
    products = load_products()
    stores, probabilities = load_stores()

    # Initialize transaction generator
    simulator = TransactionSimulator(products, stores, probabilities)

    # Initialize Kafka Producer
    producer = connect_kafka()

    # Produce transactions
    produce_transactions(simulator, producer)


if __name__ == "__main__":
    main()
