import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery, storage
from datetime import datetime

st.title("Metric Dashboards")
    
# Date selection
col1, col2, col3 = st.columns(3)
with col1:
    start_date = st.date_input("Start Date", datetime(2023, 1, 1))
with col2:
    end_date = st.date_input("End Date", datetime.today())
with col3:
    period = st.selectbox("Period", ["Week", "Month"])

# Fetch metrics data
metrics_df = fetch_metrics(start_date, end_date)

# Create 4 rows x 2 columns layout
metrics = ['Metric 1', 'Metric 2', 'Metric 3', 'Metric 4', 
            'Metric 5', 'Metric 6', 'Metric 7', 'Metric 8']

for i in range(0, 8, 2):
    cols = st.columns(2)
    for j in range(2):
        with cols[j]:
            metric_name = metrics[i+j]
            filtered_df = metrics_df[metrics_df['metric_name'] == metric_name]
            
            fig = px.line(
                filtered_df,
                x='timestamp',
                y='value',
                title=metric_name,
                labels={'value': 'Output', 'timestamp': period}
            )
            st.plotly_chart(fig, use_container_width=True)