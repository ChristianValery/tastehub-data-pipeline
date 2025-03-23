"""
This module contains classes to generate random sales transactions.

Classes:
--------
Product
    A class to represent a product.
Store
    A class to represent a store.
TransactionItem
    A class to represent a sales transaction item.
Transaction
    A class to represent a sales transaction.
"""


from datetime import datetime
import random
from typing import List, NamedTuple
from multiprocessing import Pool

import pandas as pd
import numpy as np
from tqdm import tqdm


class Product(NamedTuple):
    """
    A class to represent a product.

    Attributes:
    -----------
    id : int
        The product ID.
    name : str
        The product name.
    price : float
        The product price.
    category : str
        The product category.
    """
    id: int
    name: str
    price: float
    category: str

class Store(NamedTuple):
    """
    A class to represent a store.

    Attributes:
    -----------
    id : int
        The store ID.
    name : str
        The store name.
    city : str
        The store city.
    """
    id: int
    name: str
    city: str

class TransactionItem(NamedTuple):
    """
    A class to represent a sales transaction item.

    Attributes:
    -----------
    product_id : int
        The product ID.
    quantity : int
        The quantity of the product.
    amount : float
        The amount of the product.
    """
    product_id: int
    quantity: int
    amount: float


class Transaction(NamedTuple):
    """
    A class to represent a sales transaction.

    Attributes:
    -----------
    transaction_id : int
        The transaction ID.
    store_id : int
        The store ID.
    timestamp : str
        The transaction timestamp.
    total_amount : float
        The total amount of the transaction.
    
    """
    store_id: int
    timestamp: str
    total_amount: float
    items: List[TransactionItem]


def generate_transactions_for_day_wrapper(args):
    """
    Wrapper function to call generate_transactions_for_day with additional arguments.
    """
    simulator, date, base_transactions_per_day = args
    return simulator.generate_transactions_for_day(date, base_transactions_per_day)



class TransactionSimulator:
    """
    This class simulates sales transactions based on products, stores, and probabilities.

    Attributes:
    -----------
    products : List[Product]
        A list of products.
    stores : List[Store]
        A list of stores.
    probabilities : List[float]
        A list of probabilities for each store.
    """

    def __init__(self,
                 products: List[Product],
                 stores: List[Store],
                 store_probabilities: List[float] = None,
                 product_probabilities: List[float] = None):
        self.products = products
        self.stores = stores
        if store_probabilities is None:
            store_probabilities = [1 / len(stores)] * len(stores)
        self.store_probabilities = store_probabilities
        if product_probabilities is None:
            product_probabilities = [1 / len(products)] * len(products)
        self.product_probabilities = product_probabilities

    def get_transaction_probability(self, timestamp: datetime) -> float:
        """
        Determines the probability of transactions occurring based on time, day, and month.
        """
        hour = timestamp.hour
        day_of_week = timestamp.weekday()
        month = timestamp.month

        # Base probabilities (adjust for morning/lunch/dinner rushes)
        hour_weights = {
            **dict.fromkeys(range(6, 9), 1.5),  # Morning rush (6 AM - 9 AM)
            **dict.fromkeys(range(11, 14), 2.0),  # Lunch rush (11 AM - 2 PM)
            **dict.fromkeys(range(17, 20), 1.8),  # Evening rush (5 PM - 8 PM)
        }

        # Stores open from 06:00 to 21:00, closed on Sundays
        if hour < 6 or hour >= 21 or day_of_week == 6:
            return 0.0, {}

        # Lower transactions on weekends, closed on Sundays
        day_weights = {0: 1.0, 1: 1.2, 2: 1.2, 3: 1.3,
                       4: 1.5, 5: 1.0, 6: 0.0}  # Sunday closed

        # Seasonal variations
        summer_months = {6, 7, 8}
        winter_months = {12, 1, 2}
        category_boost = {
            "Cold Drinks": 2.0 if month in summer_months else 0.8,
            "Hot Drinks": 2.0 if month in winter_months else 1.0
        }

        base_prob = hour_weights.get(hour, 1.0) * day_weights[day_of_week]
        return base_prob, category_boost

    def generate_transaction(self, timestamp: datetime = None) -> Transaction:
        """
        Generates a more realistic sales transaction.
        """
        if timestamp is None:
            timestamp = datetime.now()

        store_index = np.random.choice(
            len(self.stores), p=self.store_probabilities)
        store = self.stores[store_index]
        base_prob, category_boost = self.get_transaction_probability(timestamp)

        if base_prob == 0.0:
            return None  # No transactions when stores are closed

        num_products = max(1, np.random.poisson(
            3) if random.random() < base_prob else 1)
        product_indices = np.random.choice(
            len(self.products),
            num_products,
            replace=False,
            p=self.product_probabilities)
        chosen_products = [self.products[i] for i in product_indices]

        quantity_choices = [1, 2, 3, 4, 5, 6]
        quantity_weights = [0.6, 0.2, 0.1, 0.07, 0.02, 0.01]
        quantities = random.choices(
            quantity_choices, weights=quantity_weights, k=num_products)
        quantities = [random.randint(1, 5) for _ in chosen_products]
        transaction_items = [
            TransactionItem(p.id, q, p.price * q *
                            category_boost.get(p.category, 1.0))
            for p, q in zip(chosen_products, quantities)
        ]

        total_amount = sum(item.amount for item in transaction_items)
        return Transaction(store.id,
                           timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                           total_amount,
                           transaction_items)

    def generate_transactions_for_day(
            self,
            current_date: datetime,
            base_transactions_per_day: int) -> List[Transaction]:
        """
        Generate transactions for a single day.
        """
        if current_date.weekday() == 6:  # Skip Sundays
            return []

        day_weights = {0: 1.0, 1: 1.2, 2: 1.2, 3: 1.3, 4: 1.5, 5: 1.0, 6: 0.0}
        hour_weights = {hour: 1.0 for hour in range(6, 21)}

        for h in range(6, 9):
            hour_weights[h] = 1.5  # Morning rush
        for h in range(11, 14):
            hour_weights[h] = 2.0  # Lunch rush
        for h in range(17, 20):
            hour_weights[h] = 1.8  # Evening rush

        total_hour_weight = sum(hour_weights.values())
        daily_target = int(base_transactions_per_day *
                        day_weights[current_date.weekday()])

        transactions = []
        transactions_per_hour = {
            hour: int((hour_weights[hour] / total_hour_weight) * daily_target)
            for hour in range(6, 21)
        }

        for hour, num_tx in transactions_per_hour.items():
            for _ in range(num_tx):
                timestamp = current_date.replace(
                    hour=hour, minute=random.randint(0, 59))
                transaction = self.generate_transaction(timestamp)
                if transaction:
                    transactions.append(transaction)

        return transactions

    def generate_historical_transactions(
            self,
            start_date: str,
            end_date: str,
            base_transactions_per_day: int = 10_000) -> List[Transaction]:
        """
        Generates historical transactions across multiple processes.
        """
        date_range = pd.date_range(start=start_date, end=end_date).to_list()

        with Pool() as pool:
            results = list(tqdm(
                pool.imap(
                    generate_transactions_for_day_wrapper,
                    # Pass the argument
                    [(self, date, base_transactions_per_day) for date in date_range]),
                total=len(date_range),
                desc="Generating transactions"
            ))
        return [tx for sublist in results for tx in sublist]  # Flatten results
