"""
This module handles all visualization functions for the dashboard.
"""


import plotly.express as px
import plotly.graph_objects as go
import streamlit as st


def create_sales_map(data, lat_col, lon_col, size_col, hover_name, title):
    """
    Create a mapbox scatter plot visualization.
    """
    fig = px.scatter_map(
        data, lat=lat_col, lon=lon_col, size=size_col, color=size_col,
        hover_name=hover_name, hover_data=['total_sales'], title=title,
        color_continuous_scale="reds", size_max=30, zoom=6
    )
    fig.update_layout(
        mapbox_style="carto-positron",
        mapbox_center={"lat": 50.85, "lon": 4.35},  # Centered on Belgium
        margin={"r": 0, "t": 40, "l": 0, "b": 0}
    )
    return fig


def create_time_series(pivot_df, x_col, entities, name_col, value_col, title):
    """
    Create a time series plot for multiple entities.
    """
    fig = go.Figure()
    for entity in entities:
        if entity in pivot_df.columns:
            fig.add_trace(go.Scatter(
                x=pivot_df[x_col], y=pivot_df[entity],
                mode='lines+markers', name=entity
            ))
    fig.update_layout(
        title=title, xaxis_title="Time", yaxis_title=value_col,
        legend_title=name_col, hovermode="x unified"
    )
    return fig


def display_metrics(data, date_col, value_cols, label_suffixes):
    """
    Display metric cards with current values and trends.
    """
    if data.empty:
        st.warning("No data available for metrics.")
        return
    sorted_data = data.sort_values(date_col, ascending=False)
    if len(sorted_data) >= 2:
        current = sorted_data.iloc[0]
        previous = sorted_data.iloc[1]
        cols = st.columns(len(value_cols))
        for i, (col, suffix) in enumerate(zip(value_cols, label_suffixes)):
            current_val = current[col]
            prev_val = previous[col]
            delta = current_val - prev_val
            percentage = (delta / prev_val * 100) if prev_val != 0 else 0
            formatted_val = f"€{current_val:,.2f}" if 'sales' in col else f"{int(current_val)}"
            cols[i].metric(
                label=f"{col.replace('_', ' ').title()} {suffix}",
                value=formatted_val, delta=f"{percentage:.1f}%"
            )
    else:
        cols = st.columns(len(value_cols))
        for i, (col, suffix) in enumerate(zip(value_cols, label_suffixes)):
            cols[i].metric(
                label=f"{col.replace('_', ' ').title()} {suffix}", value="N/A")
