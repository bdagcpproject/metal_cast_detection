import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from datetime import datetime
import google.auth
import plotly.graph_objects as go

st.title("📈 Metrics Dashboard")

# --- Setup BigQuery client ---
credentials, project_id = google.auth.default()
bq_client = bigquery.Client(credentials=credentials, project=project_id)

# --- Sidebar filters ---
st.sidebar.header("Filter Options")
start_date = st.sidebar.date_input("Start Date", datetime(2024, 1, 1))
end_date = st.sidebar.date_input("End Date", datetime.today())
agg_type = st.sidebar.radio("Aggregation Level", ["Weekly", "Monthly"])

# --- Helper: Fetch BigQuery Data ---
def fetch_bq_data(query: str) -> pd.DataFrame:
    df = bq_client.query(query).to_dataframe()
    df["aggregation_start"] = pd.to_datetime(df["aggregation_start"])
    df["aggregation_end"] = pd.to_datetime(df["aggregation_end"])

    df = df[(df["aggregation_start"].dt.date >= start_date) &
            (df["aggregation_start"].dt.date <= end_date)]

    df["aggregation_label"] = df["aggregation_start"].dt.strftime('%Y-%m-%d') + " → " + df["aggregation_end"].dt.strftime('%Y-%m-%d')
    return df

# --- Helper: Monthly Aggregation ---
def aggregate_monthly(df: pd.DataFrame) -> pd.DataFrame:
    df["month"] = df["aggregation_start"].dt.to_period("M").dt.to_timestamp()
    agg_funcs = {
        "confidence_score_min": "min",
        "confidence_score_med": "median",
        "confidence_score_mean": "mean",
        "confidence_score_max": "max",
        "inference_time_min": "min",
        "inference_time_med": "median",
        "inference_time_mean": "mean",
        "inference_time_max": "max",
        "pred_class_pass_freq": "sum",
        "pred_class_fail_freq": "sum"
    }
    grouped = df.groupby("month").agg({k: v for k, v in agg_funcs.items() if k in df.columns}).reset_index()
    grouped["aggregation_start"] = grouped["month"]
    grouped["aggregation_end"] = grouped["month"] + pd.offsets.MonthEnd(0)
    grouped["aggregation_label"] = grouped["aggregation_start"].dt.strftime('%Y-%m')
    return grouped.drop(columns=["month"])

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

    if agg_type == "Monthly":
        df = aggregate_monthly(df)

    st.subheader(f"{agg_type} Median & Max Confidence Score Trend")
    fig1 = px.line(df, x="aggregation_start", y=[ "confidence_score_med", "confidence_score_mean"],
                   hover_name="aggregation_label", labels={"aggregation_start": agg_type, "value": "Confidence"})
    st.plotly_chart(fig1, use_container_width=True)

    st.subheader("Min-Max Confidence Range")

    fig_band = go.Figure()

    # Min Confidence Line
    fig_band.add_trace(go.Scatter(
        x=df["aggregation_start"],
        y=df["confidence_score_min"],
        mode="lines",
        name="Min Confidence",
        line=dict(color='orange')
    ))

    # Max Confidence Line
    fig_band.add_trace(go.Scatter(
        x=df["aggregation_start"],
        y=df["confidence_score_max"],
        mode="lines",
        name="Max Confidence",
        line=dict(color='green')
    ))

    fig_band.update_layout(
        xaxis_title=agg_type,
        yaxis_title="Confidence",
        showlegend=True
    )

    st.plotly_chart(fig_band, use_container_width=True)

    st.subheader("Mean Confidence Score Distribution")
    df["binned_confidence"] = pd.cut(df["confidence_score_mean"], bins=10).astype(str)
    hist_df = df.groupby("binned_confidence").size().reset_index(name="count")
    fig_hist = px.bar(hist_df, x="binned_confidence", y="count", labels={"binned_confidence": "Confidence Range", "count": "Count"})
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

    if agg_type == "Monthly":
        df = aggregate_monthly(df)

    st.subheader(f"{agg_type} Inference Time Trends")
    fig2 = px.line(df, x="aggregation_start", y=["inference_time_min", "inference_time_med", "inference_time_mean", "inference_time_max"],
                   hover_name="aggregation_label", labels={"aggregation_start": agg_type, "value": "Inference Time (ms)"})
    st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Mean Inference Time Distribution")
    df["binned_time"] = pd.cut(df["inference_time_mean"], bins=10).astype(str)
    hist_df2 = df.groupby("binned_time").size().reset_index(name="count")
    fig_hist2 = px.bar(hist_df2, x="binned_time", y="count", labels={"binned_time": "Time Range (ms)", "count": "Count"})
    st.plotly_chart(fig_hist2, use_container_width=True)

# --- Tab 3: Prediction Classes ---
with tab3:
    query = """
    SELECT 
        aggregation_start,
        aggregation_end,
        pred_class_pass_freq AS OK, 
        pred_class_fail_freq AS Defect
    FROM `cast-defect-detection.cast_defect_detection.prediction_class_metrics`
    ORDER BY aggregation_start
    """
    df = fetch_bq_data(query)

    if agg_type == "Monthly":
        df = aggregate_monthly(df)

    st.subheader("Pass vs Fail Frequency (Stacked Bar)")
    df_melted = df.melt(
        id_vars=["aggregation_start", "aggregation_label"],
        value_vars=["OK", "Defect"],
        var_name="Result Type",
        value_name="Count"
    )
    fig_bar = px.bar(df_melted, x="aggregation_start", y="Count", color="Result Type",
                     barmode="stack", hover_name="aggregation_label",
                     labels={"aggregation_start": agg_type})
    st.plotly_chart(fig_bar, use_container_width=True)

    st.subheader("Failure Rate Over Time")
    df["fail_rate"] = df["Defect"] / (df["OK"] + df["Defect"])* 100
    fig_line = px.line(df, x="aggregation_start", y="fail_rate",
                       labels={"aggregation_start": agg_type, "fail_rate": "Fail Rate"})
    st.plotly_chart(fig_line, use_container_width=True)

    st.subheader("Pass vs Fail Distribution")
    total = df[["OK", "Defect"]].sum().reset_index()
    total.columns = ["Class", "Total"]
    fig_pie = px.pie(total, names="Class", values="Total", title="Total Class Distribution")
    st.plotly_chart(fig_pie, use_container_width=True)
