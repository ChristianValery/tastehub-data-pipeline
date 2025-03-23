"""
This script creates the tables in the database.
It also populates the products and stores tables with some initial data.
"""


import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Index
import pandas as pd

from models import (Base, Item, Product, Store, Transaction,
                    StreamingProductSales, StreamingStoreSales)


# Load the environment variables
load_dotenv()
# Get the database URL from the environment variable
DATABASE_URL = os.getenv('DATABASE_URL')

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def create_tables():
    """
    Create the tables in the database.
    """
    # Delete all tables if they already exist
    Base.metadata.drop_all(engine)
    # Create the tables
    Base.metadata.create_all(engine)

    # Add indexes to frequently queried columns
    Index("idx_transactions_timestamp", Transaction.timestamp).create(bind=engine)
    Index("idx_transactions_store_id", Transaction.store_id).create(bind=engine)
    Index("idx_items_transaction_id", Item.transaction_id).create(bind=engine)
    Index("idx_items_product_id", Item.product_id).create(bind=engine)

    # Add indexes to streaming tables
    Index("idx_streaming_product_window",
          StreamingProductSales.window_end).create(bind=engine)
    Index("idx_streaming_product_id",
          StreamingProductSales.product_id).create(bind=engine)
    Index("idx_streaming_store_window",
          StreamingStoreSales.window_end).create(bind=engine)
    Index("idx_streaming_store_id",
          StreamingStoreSales.store_id).create(bind=engine)

    print("Tables created successfully!")

def populate_tables(db_session):
    """
    Populate the products and stores tables with some initial data.
    """
    products_df = pd.read_parquet('seeds/products.parquet')
    products = [
        Product(
            name=row['name'],
            category=row['category'],
            price=row['price'],
            probability=row['probability']
        ) for _, row in products_df.iterrows()
    ]
    db_session.add_all(products)

    stores_df = pd.read_parquet('seeds/stores.parquet')
    stores = [
        Store(
            name=row['name'],
            city=row['city'],
            population=row['population'],
            latitude=row['latitude'],
            longitude=row['longitude']
        ) for _, row in stores_df.iterrows()
    ]
    db_session.add_all(stores)

    db_session.commit()
    print("Tables products and stores populated successfully!")


if __name__ == '__main__':
    create_tables()
    populate_tables(SessionLocal())
