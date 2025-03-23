"""
This script generates transactions in real-time and sends them to Kafka.

It also inserts historical transactions into the database.

The transactions are generated based on products, stores, and probabilities.

The transactions are sent to the Kafka topic 'transactions_topic'.

The historical transactions are inserted into the 'transactions' and 'items' tables in the database.
"""


import os
from datetime import datetime, timedelta
import logging
from typing import List, Tuple
import calendar

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from tqdm import tqdm

from transaction_generator import (
    Product,
    Store,
    TransactionSimulator)
from models import Transaction, Item


# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
BATCH_SIZE = 100


# Initialize database connection
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def load_products() -> Tuple[List[Product], List[float]]:
    """
    Load products information from the database.
    """
    try:
        products_df = pd.read_sql_table('products', engine)
        return [
            Product(row['product_id'], row['name'],
                    row['price'], row['category'])
            for _, row in products_df.iterrows()
        ], products_df["probability"].tolist()
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


def get_date_intervals(start_date: str) -> List[Tuple[str, str]]:
    """
    Generates date intervals from the start date to the day before today.

    Parameters:
    -----------
    start_date : str
        The start date in "YYYY-MM-DD" format.
    
    Returns:
    --------
    intervals : list of tuples
        A list of tuples containing the start and end date of each interval.
    """
    intervals = []
    start = datetime.strptime(start_date, "%Y-%m-%d")
    today = datetime.today()
    yesterday = today - timedelta(days=1)

    # First interval
    last_day_of_start_month = datetime(
        start.year, start.month, calendar.monthrange(start.year, start.month)[1])

    # If start date and yesterday are in the same month, handle as a special case
    if start.year == yesterday.year and start.month == yesterday.month:
        intervals.append((start.strftime("%Y-%m-%d"),
                         yesterday.strftime("%Y-%m-%d")))
        return intervals

    intervals.append(
        (start.strftime("%Y-%m-%d"), last_day_of_start_month.strftime("%Y-%m-%d"))
    )

    # Move to the next month
    current = last_day_of_start_month + timedelta(days=1)

    # Handle full months until the month before yesterday's month
    while current.year < yesterday.year or \
        (current.year == yesterday.year and current.month < yesterday.month):
        last_day_of_month = datetime(
            current.year,
            current.month,
            calendar.monthrange(current.year, current.month)[1])
        intervals.append(
            (current.strftime("%Y-%m-%d"), last_day_of_month.strftime("%Y-%m-%d")))
        current = last_day_of_month + timedelta(days=1)

    # Handle the last interval only if we've reached yesterday's month
    if current.year == yesterday.year and current.month == yesterday.month:
        intervals.append((current.strftime("%Y-%m-%d"),
                         yesterday.strftime("%Y-%m-%d")))

    return intervals


def insert_historical_transactions(
        db_session,
        simulator: TransactionSimulator,
        start_date: str,
        end_date: str):
    """
    Generates and inserts historical transactions into the database using bulk inserts.
    """
    logging.info("Generating transactions from %s to %s ...",
                 start_date, end_date)
    transactions = simulator.generate_historical_transactions(
        start_date, end_date, base_transactions_per_day=10_000)
    transactions_data = []
    items_data = []

    with tqdm(total=len(transactions), desc="Inserting transactions into DB") as pbar:
        for transaction in transactions:
            transactions_data.append({
                "store_id": transaction.store_id,
                "timestamp": transaction.timestamp,
                "total_amount": transaction.total_amount
            })

        db_session.bulk_insert_mappings(Transaction, transactions_data)
        db_session.commit()

        transaction_ids = db_session.execute(
            text("SELECT transaction_id FROM transactions WHERE timestamp >= :start_time"),
            {"start_time": start_date}
        ).fetchall()

        tx_id_list = [tx_id[0] for tx_id in transaction_ids]
        if len(tx_id_list) != len(transactions_data):
            logging.error("Mismatch in transaction ID count!")

        for i, transaction in enumerate(transactions):
            tx_id = tx_id_list[i]
            for item in transaction.items:
                items_data.append({
                    "transaction_id": tx_id,
                    "product_id": item.product_id,
                    "quantity": item.quantity,
                    "amount": item.amount
                })

        db_session.bulk_insert_mappings(Item, items_data)
        db_session.commit()
        pbar.update(len(transactions))

    logging.info(
        "Finished inserting historical transactions from %s to %s!",
        start_date,
        end_date)
    db_session.close()


def main():
    """
    Main function to generate transactions and send them to Kafka.
    """
    # Load products and stores
    products, product_probabilities = load_products()
    stores, store_probabilities = load_stores()

    # Initialize transaction generator
    simulator = TransactionSimulator(
        products, stores, store_probabilities, product_probabilities)

    date_intervals = get_date_intervals("2024-01-01")

    for start_date, end_date in date_intervals:
        insert_historical_transactions(
            SessionLocal(),
            simulator,
            start_date,
            end_date)


if __name__ == "__main__":
    main()
