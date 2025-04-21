import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from datetime import datetime
import google.auth

#st.set_page_config(layout="wide")
st.title("📈 Weekly Metrics Dashboard")

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

    df = df[(df["aggregation_start"].dt.date >= start_date) & 
            (df["aggregation_start"].dt.date <= end_date)]

    df["aggregation_label"] = df["aggregation_start"].dt.strftime('%Y-%m-%d') + " → " + df["aggregation_end"].dt.strftime('%Y-%m-%d')
    return df

# --- Tabs ---
tab1, tab2, tab3 = st.tabs(["Confidence Scores", "Inference Time", "Prediction Classes"])

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

    # Line chart
    st.subheader("Weekly Confidence Score Trends")
    fig1 = px.line(df, x="aggregation_start", y=["confidence_score_min", "confidence_score_med", "confidence_score_mean", "confidence_score_max"],
                   hover_name="aggregation_label", labels={"aggregation_start": "Week", "value": "Confidence"})
    st.plotly_chart(fig1, use_container_width=True)

    # Area chart for min-max band
    st.subheader("Confidence Score Range (Min to Max)")
    fig_band = px.area(df, x="aggregation_start", y=["confidence_score_min", "confidence_score_max"],
                       labels={"aggregation_start": "Week", "value": "Score"})
    st.plotly_chart(fig_band, use_container_width=True)

    # Histogram of mean confidence
    st.subheader("Distribution of Mean Confidence Scores")
    fig_hist = px.histogram(df, x="confidence_score_mean", nbins=20, labels={"confidence_score_mean": "Mean Confidence"})
    st.plotly_chart(fig_hist, use_container_width=True)

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

    st.subheader("Weekly Inference Time Trends")
    fig2 = px.line(df, x="aggregation_start", y=["inference_time_min", "inference_time_med", "inference_time_mean", "inference_time_max"],
                   hover_name="aggregation_label", labels={"aggregation_start": "Week", "value": "Inference Time (ms)"})
    st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Distribution of Mean Inference Time")
    fig_hist2 = px.histogram(df, x="inference_time_mean", nbins=20, labels={"inference_time_mean": "Mean Inference Time (ms)"})
    st.plotly_chart(fig_hist2, use_container_width=True)

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

    st.subheader("Pass vs Fail Frequency (Stacked Bar)")
    df_melted = df.melt(
        id_vars=["aggregation_start", "aggregation_label"],
        value_vars=["pred_class_pass_freq", "pred_class_fail_freq"],
        var_name="Result Type",
        value_name="Count"
    )
    fig_bar = px.bar(df_melted, x="aggregation_start", y="Count", color="Result Type",
                     barmode="stack", hover_name="aggregation_label",
                     labels={"aggregation_start": "Week"})
    st.plotly_chart(fig_bar, use_container_width=True)

    st.subheader("Failure Rate Over Time")
    df["fail_rate"] = df["pred_class_fail_freq"] / (df["pred_class_pass_freq"] + df["pred_class_fail_freq"])
    fig_line = px.line(df, x="aggregation_start", y="fail_rate",
                       labels={"aggregation_start": "Week", "fail_rate": "Fail Rate"})
    st.plotly_chart(fig_line, use_container_width=True)

    st.subheader("Pass vs Fail Distribution")
    total = df[["pred_class_pass_freq", "pred_class_fail_freq"]].sum().reset_index()
    total.columns = ["Class", "Total"]
    fig_pie = px.pie(total, names="Class", values="Total", title="Total Class Distribution")
    st.plotly_chart(fig_pie, use_container_width=True)
