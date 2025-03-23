"""
This module contains the main application for the TasteHub dashboard.
"""

import os
import logging
from datetime import datetime, timedelta
import time
import toml

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from visualizations import create_sales_map, create_time_series, display_metrics
from database import (get_cities_list,
                      get_categories_list,
                      get_sales_data,
                      get_product_sales_data,
                      get_real_time_product_sales,
                      get_real_time_store_sales)


# Configure logging
logging.basicConfig(level=logging.ERROR,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# App configuration
st.set_page_config(page_title="TasteHub Sales Dashboard",
                   page_icon="🇧🇪", layout="wide", initial_sidebar_state="expanded")


class TasteHubDashboard:
    """
    Main class to handle the TasteHub dashboard application with all pages.
    """

    def __init__(self):
        self.init_sidebar()
        self.page = st.sidebar.radio(
            "Select View", ["Historical Analytics", "Real-Time Analytics", "Settings"])

        # Handle page navigation
        if self.page == "Historical Analytics":
            self.historical_page()
        elif self.page == "Real-Time Analytics":
            self.realtime_page()
        else:
            self.settings_page()

    def init_sidebar(self):
        """Initialize the sidebar elements common to all pages"""
        st.sidebar.title("🇧🇪 TasteHub Dashboard")

        # Theme selector in the sidebar
        self.theme = st.sidebar.selectbox(
            "Select Theme",
            ["Light", "Dark", "Dark Blue"],
            key="theme_selector")

        # Button to apply the theme
        if st.sidebar.button("Apply Theme"):
            self.switch_theme(self.theme)

    def switch_theme(self, theme_name):
        """
        Update the config.toml file to switch themes
        """
        config_path = os.path.join(".streamlit", "config.toml")

        # Create default config if it doesn't exist
        if not os.path.exists(config_path):
            os.makedirs(os.path.dirname(config_path), exist_ok=True)
            default_config = {
                "theme": {
                    "primaryColor": "#F63366",
                    "backgroundColor": "#FFFFFF",
                    "secondaryBackgroundColor": "#F0F2F6",
                    "textColor": "#262730",
                    "font": "sans serif"
                }
            }
            with open(config_path, "w", encoding="utf-8") as f:
                toml.dump(default_config, f)

        # Load existing config
        config = toml.load(config_path)

        # Make sure 'theme' section exists
        if 'theme' not in config:
            config['theme'] = {}

        # Apply the selected theme
        if theme_name == "Light":
            config["theme"] = {
                "primaryColor": "#F63366",
                "backgroundColor": "#FFFFFF",
                "secondaryBackgroundColor": "#F0F2F6",
                "textColor": "#262730",
                "font": "sans serif"
            }
        elif theme_name == "Dark":
            config["theme"] = {
                "primaryColor": "#FF4B4B",
                "backgroundColor": "#0E1117",
                "secondaryBackgroundColor": "#262730",
                "textColor": "#FAFAFA",
                "font": "sans serif"
            }
        elif theme_name == "Dark Blue":
            config["theme"] = {
                "primaryColor": "#4B8BFF",
                "backgroundColor": "#0A192F",
                "secondaryBackgroundColor": "#172A46",
                "textColor": "#F8F8F2",
                "font": "sans serif"
            }

        # Write back to config.toml
        with open(config_path, "w", encoding="utf-8") as f:
            toml.dump(config, f)
            st.success(
                f"Theme updated to {theme_name}! Please refresh the page to see changes.")

    def historical_page(self):
        """Handle the historical analytics page"""
        st.sidebar.header("Filter Options")

        # Date filters
        default_start, default_end = datetime.now() - timedelta(days=7), datetime.now()
        self.start_date = st.sidebar.date_input("Start Date", default_start)
        self.end_date = st.sidebar.date_input("End Date", default_end)

        # City and category filters
        cities = get_cities_list()
        self.selected_cities = st.sidebar.multiselect("Filter by City", cities)

        categories = get_categories_list()
        self.selected_categories = st.sidebar.multiselect(
            "Filter by Category", categories)

        # Add apply button for filters
        if st.sidebar.button("Apply Filters"):
            self.display_historical_data()
        else:
            # Show default view on first load
            if "historical_first_load" not in st.session_state:
                st.session_state["historical_first_load"] = True
                self.display_historical_data()

    def display_historical_data(self):
        """Display historical data with applied filters"""
        st.title("🇧🇪 TasteHub Historical Sales Dashboard")

        # Show applied filters
        filter_text = []
        if self.selected_cities:
            filter_text.append(f"Cities: {', '.join(self.selected_cities)}")
        if self.selected_categories:
            filter_text.append(
                f"Categories: {', '.join(self.selected_categories)}")
        if filter_text:
            st.info(f"Applied filters: {' | '.join(filter_text)}")

        # Date range info
        st.info(f"Date range: {self.start_date} to {self.end_date}")

        with st.spinner("Loading historical sales data..."):
            df = get_sales_data(self.start_date, self.end_date,
                                self.selected_cities, self.selected_categories)
            product_sales_df = get_product_sales_data(
                self.start_date, self.end_date, self.selected_cities, self.selected_categories)

        if df.empty:
            st.warning(
                "No sales data available for the selected period and filters.")
            return

        df["timestamp"] = pd.to_datetime(df["timestamp"])

        st.subheader("Key Metrics")
        total_sales, total_transactions = df["total_amount"].sum(
        ), df["timestamp"].nunique()
        avg_transaction, median_transaction = df["total_amount"].mean(
        ), df["total_amount"].median()

        col1, col2 = st.columns(2)
        col1.metric("Total Sales Revenue", f"€{total_sales:,.2f}")
        col2.metric("Number of Transactions", f"{total_transactions:,}")
        col3, col4 = st.columns(2)
        col3.metric("Average Transaction Amount", f"€{avg_transaction:.2f}")
        col4.metric("Median Transaction Amount", f"€{median_transaction:.2f}")

        # Create a single set of tabs for all analyses
        tabs = st.tabs(
            ["Geographic Analysis", "Time Analysis", "Product Analysis"])

        # Fill each tab with appropriate content
        with tabs[0]:
            self.display_geographic_analysis(df)

        with tabs[1]:
            self.display_time_analysis(df)

        with tabs[2]:
            self.display_product_analysis(df, product_sales_df)

    def display_geographic_analysis(self, df):
        """Display the geographic analysis content"""
        sales_by_city = df.groupby("city").agg(
            total_sales=("total_amount", "sum"),
            longitude=("longitude", "mean"),
            latitude=("latitude", "mean"),
            store_name=("store_name", "first")
        ).reset_index()

        st.write("### Sales Revenue by City")
        st.plotly_chart(
            create_sales_map(
                sales_by_city,
                "latitude",
                "longitude",
                "total_sales",
                "city",
                "Sales Revenue by City"),
            use_container_width=True)

        st.write("### Sales by City")
        fig = px.bar(
            sales_by_city,
            x="city",
            y="total_sales",
            title="Sales Revenue by City",
            labels={"city": "City", "total_sales": "Total Sales (€)"},
            color="total_sales",
            color_continuous_scale="reds",
            text=sales_by_city["total_sales"].apply(lambda x: f"€{x:,.2f}"))
        fig.update_traces(texttemplate='%{text}', textposition='outside')
        st.plotly_chart(fig, use_container_width=True)

    def display_time_analysis(self, df):
        """Display the time analysis content"""
        st.write("### Sales by Hour of the Day")
        df["hour"] = df["timestamp"].dt.hour
        sales_by_hour = df.groupby("hour")["total_amount"].sum().reset_index()

        if (self.end_date - self.start_date).days > 1:
            prev_start = self.start_date - (self.end_date - self.start_date)
            prev_end = self.start_date - timedelta(days=1)
            prev_df = get_sales_data(prev_start, prev_end)

            if not prev_df.empty:
                prev_df["timestamp"] = pd.to_datetime(prev_df["timestamp"])
                prev_df["hour"] = prev_df["timestamp"].dt.hour
                prev_sales_by_hour = prev_df.groupby(
                    "hour")["total_amount"].sum().reset_index()
                prev_sales_by_hour.rename(
                    columns={"total_amount": "prev_amount"}, inplace=True)
                sales_by_hour = pd.merge(
                    sales_by_hour, prev_sales_by_hour, on="hour", how="left")

                fig = go.Figure()
                fig.add_trace(
                    go.Bar(
                        x=sales_by_hour["hour"],
                        y=sales_by_hour["total_amount"],
                        name="Current Period",
                        marker_color='indianred'))
                fig.add_trace(
                    go.Bar(
                        x=sales_by_hour["hour"],
                        y=sales_by_hour["prev_amount"],
                        name="Previous Period",
                        marker_color='lightsalmon',
                        opacity=0.7))
                fig.update_layout(
                    title="Total Sales by Hour of the Day",
                    xaxis_title="Hour of the Day",
                    yaxis_title="Total Sales (€)",
                    barmode='group',
                    legend_title="Period")
            else:
                fig = px.bar(
                    sales_by_hour,
                    x="hour",
                    y="total_amount",
                    title="Total Sales by Hour of the Day",
                    labels={
                        "hour": "Hour of the Day",
                        "total_amount": "Total Sales (€)"},
                    color="total_amount", color_continuous_scale="reds")
        else:
            fig = px.bar(
                sales_by_hour,
                x="hour",
                y="total_amount",
                title="Total Sales by Hour of the Day",
                labels={
                    "hour": "Hour of the Day",
                    "total_amount": "Total Sales (€)"},
                color="total_amount",
                color_continuous_scale="reds")
        st.plotly_chart(fig, use_container_width=True)

        st.write("### Sales by Day of the Week")
        df["day_of_week"] = pd.Categorical(
            df["timestamp"].dt.day_name(),
            categories=[
                "Monday",
                "Tuesday",
                "Wednesday",
                "Thursday",
                "Friday",
                "Saturday",
                "Sunday"],
            ordered=True)
        sales_by_day = df.groupby("day_of_week", observed=False)[
            "total_amount"].sum().reset_index()
        fig = px.bar(
            sales_by_day,
            x="day_of_week",
            y="total_amount",
            title="Total Sales by Day of the Week",
            labels={
                "day_of_week": "Day of the Week",
                "total_amount": "Total Sales (€)"},
            color="total_amount",
            color_continuous_scale="reds",
            text=sales_by_day["total_amount"].apply(lambda x: f"€{x:,.2f}"))
        fig.update_traces(texttemplate='%{text}', textposition='outside')
        st.plotly_chart(fig, use_container_width=True)

        st.write("### Daily Sales Trend")
        df["date"] = df["timestamp"].dt.date
        daily_sales = df.groupby("date")["total_amount"].sum().reset_index()
        fig = px.line(
            daily_sales,
            x="date",
            y="total_amount",
            title="Daily Sales Trend",
            labels={
                "date": "Date",
                "total_amount": "Total Sales (€)"},
            markers=True)
        st.plotly_chart(fig, use_container_width=True)

    def display_product_analysis(self, df, product_sales_df):
        """Display the product analysis content"""
        st.write("### Sales by Product Category")
        sales_by_category = df.groupby(
            "category")["total_amount"].sum().reset_index()
        fig = px.pie(sales_by_category, names="category", values="total_amount",
                     title="Sales Breakdown by Category", hole=0.4)
        st.plotly_chart(fig, use_container_width=True)

        st.write("### Top Products by Sales Volume")
        top_products = product_sales_df.head(10)
        fig = px.bar(
            top_products,
            x="product_name",
            y="total_quantity_sold",
            title="Top 10 Products by Sales Volume",
            labels={
                "product_name": "Product",
                "total_quantity_sold": "Items Sold"},
            color="category",
            hover_data=["total_revenue"],
            text=top_products["total_quantity_sold"].apply(lambda x: f"{int(x):,}"))
        fig.update_traces(texttemplate='%{text}', textposition='outside')
        fig.update_layout(xaxis={'categoryorder': 'total descending'})
        st.plotly_chart(fig, use_container_width=True)

        st.write("### Detailed Product Sales Data")
        st.dataframe(product_sales_df.assign(total_revenue=lambda x: x["total_revenue"].map(
            lambda value: f"€{value:,.2f}")), use_container_width=True, hide_index=True)
        csv = product_sales_df.to_csv(index=False)
        st.download_button(
            label="Download Product Sales Data",
            data=csv,
            file_name=f"tastehub_product_sales_{self.start_date}_{self.end_date}.csv",
            mime="text/csv")

    def realtime_page(self):
        """Handle the real-time analytics page"""
        st.title("🇧🇪 TasteHub Real-Time Analytics Dashboard")

        # Real-time settings
        refresh_interval = st.sidebar.slider(
            "Auto-refresh interval (seconds)", 5, 60, 30)
        auto_refresh = st.sidebar.checkbox("Enable auto-refresh", value=True)

        # Manual refresh button
        manual_refresh = st.sidebar.button("Refresh Data Now")

        last_refresh = datetime.now()
        refresh_placeholder = st.empty()
        refresh_placeholder.info(
            f"Last refreshed: {last_refresh.strftime('%H:%M:%S')}")

        with st.spinner("Loading real-time data..."):
            real_time_product_df = get_real_time_product_sales()
            real_time_store_df = get_real_time_store_sales()

        if real_time_product_df.empty and real_time_store_df.empty:
            st.warning(
                "No real-time data available yet. Ensure the Spark streaming job is running.")
            if auto_refresh or manual_refresh:
                time.sleep(refresh_interval if auto_refresh else 1)
                st.rerun()
            return

        # Create a single set of tabs for real-time analytics
        tabs = st.tabs(["Product Analytics", "Store Analytics"])

        # Fill each tab with appropriate content
        with tabs[0]:
            self.display_product_analytics(real_time_product_df)

        with tabs[1]:
            self.display_store_analytics(real_time_store_df)

        # Show latest records
        self.display_latest_records(real_time_product_df, real_time_store_df)

        # Handle refresh logic
        if auto_refresh:
            refresh_placeholder.info(f"Last refreshed: {last_refresh.strftime('%H:%M:%S')} "
                                     f"(refreshes every {refresh_interval} seconds)")
            time.sleep(refresh_interval)
            st.rerun()
        elif manual_refresh:
            st.rerun()

    def display_product_analytics(self, real_time_product_df):
        """Display the product analytics content for real-time data"""
        if not real_time_product_df.empty:
            real_time_product_df['window_end'] = pd.to_datetime(
                real_time_product_df['window_end'])
            latest_window = real_time_product_df['window_end'].max()
            recent_data = real_time_product_df[real_time_product_df['window_end']
                                               == latest_window]

            st.subheader("Current Window Metrics")
            display_metrics(real_time_product_df.groupby('window_end').agg(
                {'total_sales': 'sum', 'total_quantity': 'sum'}).reset_index(),
                'window_end', ['total_sales', 'total_quantity'], ['(€)', '(units)'])

            st.subheader("Product Sales Over Time")
            pivot_df = real_time_product_df.pivot_table(
                index='window_end',
                columns='product_name',
                values='total_quantity',
                aggfunc='sum').fillna(0).reset_index()
            top_products = real_time_product_df.groupby(
                'product_name')['total_quantity'].sum().nlargest(5).index
            st.plotly_chart(
                create_time_series(
                    pivot_df,
                    'window_end',
                    top_products,
                    'Product',
                    'Quantity Sold',
                    "Top 5 Products - Sales Over Time"),
                use_container_width=True)

            st.subheader("Current Window Product Sales")
            if not recent_data.empty:
                fig = px.pie(
                    recent_data,
                    names='product_name',
                    values='total_quantity',
                    title=f"Product Sales Distribution ({latest_window.strftime('%H:%M:%S')})",
                    hole=0.4,
                    color='product_name',
                    hover_data=['total_sales'])
                st.plotly_chart(fig, use_container_width=True)

                st.subheader("Sales by Category (Current Window)")
                category_data = recent_data.groupby('category').agg(
                    {'total_quantity': 'sum', 'total_sales': 'sum'}).reset_index()
                fig = px.bar(
                    category_data,
                    x='category',
                    y='total_sales',
                    title=f"Category Sales ({latest_window.strftime('%H:%M:%S')})",
                    color='category',
                    text=category_data["total_sales"].apply(lambda x: f"€{x:.2f}"))
                fig.update_traces(
                    texttemplate='%{text}', textposition='outside')
                st.plotly_chart(fig, use_container_width=True)

    def display_store_analytics(self, real_time_store_df):
        """Display the store analytics content for real-time data"""
        if not real_time_store_df.empty:
            real_time_store_df['window_end'] = pd.to_datetime(
                real_time_store_df['window_end'])
            latest_window = real_time_store_df['window_end'].max()
            recent_store_data = real_time_store_df[real_time_store_df['window_end']
                                                   == latest_window]

            st.subheader("Store Performance Metrics")
            if not recent_store_data.empty:
                top_store = recent_store_data.loc[recent_store_data['total_sales'].idxmax(
                )]
                top_city = recent_store_data.groupby(
                    'city')['total_sales'].sum().idxmax()

                col1, col2 = st.columns(2)
                col1.metric(
                    "Top Store",
                    f"{top_store['store_name']}",
                    f"€{top_store['total_sales']:.2f}")
                col2.metric(
                    "Top City",
                    f"{top_city}",
                    f"€{recent_store_data[recent_store_data['city'] == top_city]['total_sales'].sum():.2f}"
                )

            st.subheader("Current Sales by Location")
            if not recent_store_data.empty:
                st.plotly_chart(
                    create_sales_map(
                        recent_store_data,
                        "latitude",
                        "longitude",
                        "total_sales",
                        "store_name",
                        f"Store Sales ({latest_window.strftime('%H:%M:%S')})"),
                    use_container_width=True)

            st.subheader("Top Stores Over Time")
            store_pivot_df = real_time_store_df.pivot_table(
                index='window_end',
                columns='store_name',
                values='total_sales',
                aggfunc='sum').fillna(0).reset_index()
            top_stores = real_time_store_df.groupby(
                'store_name')['total_sales'].sum().nlargest(5).index
            st.plotly_chart(
                create_time_series(
                    store_pivot_df,
                    'window_end',
                    top_stores,
                    'Store',
                    'Sales (€)',
                    "Top 5 Stores - Sales Over Time"),
                use_container_width=True)

            st.subheader("Sales by City (Current Window)")
            city_sales = recent_store_data.groupby(
                'city')['total_sales'].sum().reset_index()
            fig = px.bar(
                city_sales,
                x="city",
                y="total_sales",
                title=f"Sales by City ({latest_window.strftime('%H:%M:%S')})",
                labels={"city": "City", "total_sales": "Total Sales (€)"},
                color="total_sales",
                color_continuous_scale="greens",
                text=city_sales["total_sales"].apply(lambda x: f"€{x:.2f}"))
            fig.update_traces(texttemplate='%{text}', textposition='outside')
            st.plotly_chart(fig, use_container_width=True)

    def display_latest_records(self, real_time_product_df, real_time_store_df):
        """Display the latest real-time records in tabs"""
        st.subheader("Latest Real-time Records")
        latest_records_tabs = st.tabs(["Product Records", "Store Records"])

        with latest_records_tabs[0]:
            if not real_time_product_df.empty:
                st.dataframe(
                    real_time_product_df.sort_values(
                        'window_end', ascending=False).head(20),
                    use_container_width=True,
                    hide_index=True)

        with latest_records_tabs[1]:
            if not real_time_store_df.empty:
                st.dataframe(
                    real_time_store_df.sort_values(
                        'window_end', ascending=False).head(20),
                    use_container_width=True,
                    hide_index=True)

    def settings_page(self):
        """Handle the settings page"""
        st.title("🇧🇪 TasteHub Dashboard Settings")
        st.write("### Manage Application Settings and Preferences")

        st.subheader("Database Connection")
        st.write("""
        The dashboard retrieves data from a secure database. The connection settings are managed through 
        Streamlit secrets, ensuring data security and integrity. For modifications or troubleshooting, 
        please contact the IT department.
        """)

        st.subheader("Cache Settings")
        st.write("""
        To improve performance, TasteHub Dashboard caches frequently accessed data. If you encounter outdated 
        information, clearing the cache may resolve the issue.
        """)
        if st.button("Clear Cache"):
            st.cache_data.clear()
            st.success("Cache cleared successfully!")

        st.subheader("Data Export Options")
        st.write("""
        Select the default format for exporting analytics data. Supported formats include:
        - **CSV** (Comma-separated values, compatible with Excel and other tools)
        - **Excel** (.xlsx format for structured data management)
        - **JSON** (Useful for API integrations and development purposes)
        """)
        export_format = st.selectbox("Default Export Format", [
                                     "CSV", "Excel", "JSON"])
        st.session_state["export_format"] = export_format

        st.subheader("About TasteHub Dashboard")
        st.write("""
        The TasteHub Dashboard provides insights into historical and real-time sales data, helping businesses 
        make informed decisions. It is designed for restaurant managers, analysts, and stakeholders to track trends 
        and optimize performance.
        
        - **Version:** 1.0.0
        - **Last Updated:** March 2025
        - **Support Contact:** IT Department (support@tastehub.com)
        """)


def main():
    """
    Main function to run the TasteHub dashboard application.
    """
    TasteHubDashboard()


if __name__ == "__main__":
    main()
