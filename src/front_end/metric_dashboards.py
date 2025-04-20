import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from datetime import datetime
import google.auth

st.title("📊 Cast Defect Detection - Weekly Metrics Dashboard")

# --- Setup BigQuery client ---
credentials, project_id = google.auth.default()
bq_client = bigquery.Client(credentials=credentials, project=project_id)

# --- Sidebar filters ---
st.sidebar.header("Filter Options")
start_date = st.sidebar.date_input("Start Date", datetime(2024, 1, 1))
end_date = st.sidebar.date_input("End Date", datetime.today())

# --- Helper function ---
def fetch_bq_data(query: str) -> pd.DataFrame:
    df = bq_client.query(query).to_dataframe()
    df["aggregation_start"] = pd.to_datetime(df["aggregation_start"])
    df["aggregation_end"] = pd.to_datetime(df["aggregation_end"])
    
    # Filter by aggregation_start
    df = df[(df["aggregation_start"].dt.date >= start_date) & 
            (df["aggregation_start"].dt.date <= end_date)]

    # Add label for tooltip or x-axis
    df["aggregation_label"] = df["aggregation_start"].dt.strftime('%Y-%m-%d') + " → " + df["aggregation_end"].dt.strftime('%Y-%m-%d')
    return df

# --- Tabs ---
tab1, tab2, tab3 = st.tabs(["📈 Confidence Scores", "⚙️ Inference Time", "✅ Prediction Classes"])

# --- Tab 1: Confidence Scores ---
with tab1:
    query = """
    SELECT 
        aggregation_start,
        aggregation_end,
        confidence_score_min, 
        confidence_score_med, 
        confidence_score_mean, 
        confidence_score_max
    FROM `cast-defect-detection.cast_defect_detection.confidencescore_metrics`
    ORDER BY aggregation_start
    """
    df = fetch_bq_data(query)

    fig = px.line(
        df,
        x="aggregation_start",
        y=["confidence_score_min", "confidence_score_med", "confidence_score_mean", "confidence_score_max"],
        hover_name="aggregation_label",
        labels={"aggregation_start": "Week Start", "value": "Confidence"},
        title="Weekly Confidence Score Trends"
    )
    st.plotly_chart(fig, use_container_width=True)

# --- Tab 2: Inference Time ---
with tab2:
    query = """
    SELECT 
        aggregation_start,
        aggregation_end,
        inference_time_min, 
        inference_time_med, 
        inference_time_mean, 
        inference_time_max
    FROM `cast-defect-detection.cast_defect_detection.inference_metrics`
    ORDER BY aggregation_start
    """
    df = fetch_bq_data(query)

    fig = px.line(
        df,
        x="aggregation_start",
        y=["inference_time_min", "inference_time_med", "inference_time_mean", "inference_time_max"],
        hover_name="aggregation_label",
        labels={"aggregation_start": "Week Start", "value": "Inference Time (ms)"},
        title="Weekly Inference Time Trends"
    )
    st.plotly_chart(fig, use_container_width=True)

# --- Tab 3: Prediction Classes ---
with tab3:
    query = """
    SELECT 
        aggregation_start,
        aggregation_end,
        pred_class_pass_freq, 
        pred_class_fail_freq
    FROM `cast-defect-detection.cast_defect_detection.prediction_class_metrics`
    ORDER BY aggregation_start
    """
    df = fetch_bq_data(query)

    df_melted = df.melt(
        id_vars=["aggregation_start", "aggregation_label"],
        value_vars=["pred_class_pass_freq", "pred_class_fail_freq"],
        var_name="Result Type",
        value_name="Count"
    )

    fig = px.bar(
        df_melted,
        x="aggregation_start",
        y="Count",
        color="Result Type",
        barmode="group",
        hover_name="aggregation_label",
        title="Weekly Prediction Class Frequencies",
        labels={"aggregation_start": "Week Start"}
    )
    st.plotly_chart(fig, use_container_width=True)
