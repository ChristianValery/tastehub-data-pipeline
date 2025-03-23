"""
Database models for the tastehub database.
"""


from sqlalchemy import Column, Integer, String, Float, TIMESTAMP, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship


Base = declarative_base()


class Product(Base):
    """
    Class to represent a product.
    """
    __tablename__ = 'products'

    product_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    category = Column(String, nullable=False)
    price = Column(Float, nullable=False)
    probability = Column(Float, nullable=False)


class Store(Base):
    """
    Class to represent a store."
    """
    __tablename__ = 'stores'

    store_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    city = Column(String, nullable=False)
    population = Column(Integer, nullable=False)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)


class Transaction(Base):
    """
    Class to represent a sales transaction."
    """
    __tablename__ = 'transactions'

    transaction_id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(TIMESTAMP, nullable=False)
    store_id = Column(Integer, ForeignKey(
        'stores.store_id', ondelete="CASCADE"), nullable=False)
    total_amount = Column(Float, nullable=False)

    store = relationship("Store", backref="transactions")


class Item(Base):
    """"
    Class to represent a sales transaction item."
    """
    __tablename__ = 'items'

    item_id = Column(Integer, primary_key=True, autoincrement=True)
    transaction_id = Column(Integer, ForeignKey(
        'transactions.transaction_id', ondelete="CASCADE"), nullable=False)
    product_id = Column(Integer, ForeignKey(
        'products.product_id', ondelete="CASCADE"), nullable=False)
    quantity = Column(Integer, nullable=False)
    amount = Column(Float, nullable=False)

    transaction = relationship("Transaction", backref="items")
    product = relationship("Product", backref="items")


class StreamingProductSales(Base):
    """Class to represent real-time sales analytics by product."""
    __tablename__ = 'streaming_product_sales'

    id = Column(Integer, primary_key=True, autoincrement=True)
    window_start = Column(TIMESTAMP, nullable=False)
    window_end = Column(TIMESTAMP, nullable=False)
    product_id = Column(Integer, ForeignKey(
        'products.product_id', ondelete="CASCADE"), nullable=False)
    total_quantity = Column(Integer, nullable=False)
    total_sales = Column(Float, nullable=False)
    processed_at = Column(TIMESTAMP, nullable=False)

    product = relationship("Product", backref="streaming_sales")


class StreamingStoreSales(Base):
    """Class to represent real-time sales analytics by store."""
    __tablename__ = 'streaming_store_sales'

    id = Column(Integer, primary_key=True, autoincrement=True)
    window_start = Column(TIMESTAMP, nullable=False)
    window_end = Column(TIMESTAMP, nullable=False)
    store_id = Column(Integer, ForeignKey(
        'stores.store_id', ondelete="CASCADE"), nullable=False)
    total_quantity = Column(Integer, nullable=False)
    total_sales = Column(Float, nullable=False)
    processed_at = Column(TIMESTAMP, nullable=False)

    store = relationship("Store", backref="streaming_sales")
