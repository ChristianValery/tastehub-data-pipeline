"""
This module contains all database-related functions for the dashboard.
It handles database connections, queries, and data retrieval.
"""


import logging

import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import QueuePool

# Configure logging
logger = logging.getLogger(__name__)


@st.cache_resource
def get_database_connection():
    """
    Create and return a database connection pool.
    """
    try:
        db_url = st.secrets["db_credentials"]["DATABASE_URL"]
        engine = create_engine(db_url, poolclass=QueuePool,
                               pool_size=5, max_overflow=10)
        return engine
    except KeyError as e:
        logger.error("Missing database credentials: %s", e)
        st.error("Database credentials are missing. Please check the configuration.")
    except SQLAlchemyError as e:
        logger.error("Database connection error: %s", e)
        st.error(
            "Failed to connect to the database. Please check your configuration.")
    return None


@st.cache_data(ttl=3600)
def get_cities_list():
    """
    Get a list of all cities from the database.
    """
    engine = get_database_connection()
    if not engine:
        return []
    query = "SELECT DISTINCT city FROM stores ORDER BY city"
    try:
        df = pd.read_sql(query, engine)
        return df["city"].tolist()
    except SQLAlchemyError as e:
        logger.error("Query error: %s", e)
        return []


@st.cache_data(ttl=3600)
def get_categories_list():
    """
    Get a list of all product categories from the database.
    """
    engine = get_database_connection()
    if not engine:
        return []
    query = "SELECT DISTINCT category FROM products ORDER BY category"
    try:
        df = pd.read_sql(query, engine)
        return df["category"].tolist()
    except SQLAlchemyError as e:
        logger.error("Query error: %s", e)
        return []


@st.cache_data(ttl=3600)
def get_sales_data(starting, ending, cities=None, categories=None):
    """
    Fetch historical sales data from the database.
    """
    engine = get_database_connection()
    if not engine:
        return pd.DataFrame()
    query = f"""
        SELECT 
            t.timestamp, t.total_amount, s.city, s.name as store_name,
            s.latitude, s.longitude, p.category
        FROM transactions t
        JOIN stores s ON t.store_id = s.store_id
        JOIN items i ON t.transaction_id = i.transaction_id
        JOIN products p ON i.product_id = p.product_id
        WHERE t.timestamp BETWEEN '{starting}' AND '{ending}'
    """
    if cities and len(cities) > 0:
        city_list = "', '".join(cities)
        query += f" AND s.city IN ('{city_list}')"
    if categories and len(categories) > 0:
        category_list = "', '".join(categories)
        query += f" AND p.category IN ('{category_list}')"
    try:
        return pd.read_sql(query, engine)
    except SQLAlchemyError as e:
        logger.error("Query error: %s", e)
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def get_product_sales_data(starting, ending, cities=None, categories=None):
    """
    Fetch historical product sales data from the database.
    """
    engine = get_database_connection()
    if not engine:
        return pd.DataFrame()
    query = f"""
        SELECT 
            p.name AS product_name, p.category,
            SUM(i.quantity) AS total_quantity_sold,
            SUM(i.quantity * p.price) AS total_revenue
        FROM items i
        JOIN products p ON i.product_id = p.product_id
        JOIN transactions t ON i.transaction_id = t.transaction_id
        JOIN stores s ON t.store_id = s.store_id
        WHERE t.timestamp BETWEEN '{starting}' AND '{ending}'
    """
    if cities and len(cities) > 0:
        city_list = "', '".join(cities)
        query += f" AND s.city IN ('{city_list}')"
    if categories and len(categories) > 0:
        category_list = "', '".join(categories)
        query += f" AND p.category IN ('{category_list}')"
    query += " GROUP BY p.name, p.category ORDER BY total_quantity_sold DESC"
    try:
        return pd.read_sql(query, engine)
    except SQLAlchemyError as e:
        logger.error("Query error: %s", e)
        return pd.DataFrame()


@st.cache_data(ttl=60)
def get_real_time_product_sales():
    """
    Fetch real-time product sales data from streaming analysis.
    """
    engine = get_database_connection()
    if not engine:
        return pd.DataFrame()
    query = """
        SELECT
            p.name AS product_name, p.category, sps.window_start,
            sps.window_end, sps.total_quantity, sps.total_sales
        FROM streaming_product_sales sps
        JOIN products p ON sps.product_id = p.product_id
        WHERE sps.window_end > NOW() - INTERVAL '1 hour'
        ORDER BY sps.window_end DESC, sps.total_sales DESC
    """
    try:
        return pd.read_sql(query, engine)
    except SQLAlchemyError as e:
        logger.error("Query error: %s", e)
        return pd.DataFrame(columns=['product_name', 'category', 'window_start',
                                     'window_end', 'total_quantity', 'total_sales'])


@st.cache_data(ttl=60)
def get_real_time_store_sales():
    """
    Fetch real-time store sales data from streaming analysis.
    """
    engine = get_database_connection()
    if not engine:
        return pd.DataFrame()
    query = """
        SELECT
            s.name AS store_name, s.city, s.latitude, s.longitude,
            sss.window_start, sss.window_end, sss.total_quantity, sss.total_sales
        FROM streaming_store_sales sss
        JOIN stores s ON sss.store_id = s.store_id
        WHERE sss.window_end > NOW() - INTERVAL '1 hour'
        ORDER BY sss.window_end DESC, sss.total_sales DESC
    """
    try:
        return pd.read_sql(query, engine)
    except SQLAlchemyError as e:
        logger.error("Query error: %s", e)
        return pd.DataFrame(columns=['store_name', 'city', 'latitude', 'longitude',
                                     'window_start', 'window_end', 'total_quantity', 'total_sales'])
