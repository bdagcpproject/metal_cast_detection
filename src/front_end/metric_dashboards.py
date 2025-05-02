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
        "OK": "sum",
        "Defect": "sum"
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
        
    ## --- Plot 1: Confidence Score Trend ---
    st.subheader(f"{agg_type} Prediction Confidence Score Trend")
    
    fig1 = px.line(
        df,
        x="aggregation_start",
        y=["confidence_score_med", "confidence_score_mean"],
        labels={
            "aggregation_start": agg_type,  # X-axis label
            "value": "Confidence Score",   # Y-axis label
            "variable": "Statistics" # Legend title
        },
        markers=True,  
        symbol_sequence=['square'],
        color_discrete_sequence=["#0080FF", "#00FFFF"] 
    )

    # Add threshold line as a proper trace (enables hover)
    fig1.add_trace(
        go.Scatter(
            x=df["aggregation_start"],
            y=[0.99]*len(df),
            mode='lines',
            line=dict(color='red', width=2, dash='dot'),
            name='Target',
        )
    )

    # Update x-axis format for monthly view
    if agg_type == "Monthly":
        fig1.update_xaxes(
            tickformat="%Y-%m",  # Show as "2025-03" format
            dtick="M1"           # One tick per month
        )
    else:  # Weekly
        # Only show labels where data exists
        fig1.update_xaxes(
            tickformat="%Y-%m-%d",
            tickvals=df["aggregation_start"],  # Only show ticks where data exists
        )

    # Custom hover template 
    fig1.update_traces(
        hovertemplate=(
            "<b>%{customdata[0]}</b><br>" +  # aggregation_label as title
            "<b>%{fullData.name}</b>: %{y:.4f}<extra></extra>"  # Trace name and value
        ),
        customdata=df[['aggregation_label']]  # Pass the labels as customdata
    )

    # Rename legend entries and customize markers
    fig1.for_each_trace(lambda t: t.update(
        name="Median" if "med" in t.name else "Mean",
        marker=dict(
            size=8,  # Adjust symbol size
            line=dict(width=1, color='DarkSlateGrey')  # Add border to symbols
        )
    ) if t.name != 'Target' else None)

    # Additional styling
    fig1.update_layout(
        hovermode="x unified"
    )
    st.plotly_chart(fig1, use_container_width=True)

    ## --- Plot 2: Confidence Score Range (Max-Min) ---

    df_fig2 = df.copy()
    df_fig2["confidence_score_delta"] = df_fig2["confidence_score_max"] - df_fig2["confidence_score_min"]
    st.subheader("Confidence Score Range (Max-Min)")

    fig2 = px.line(
        df_fig2,
        x="aggregation_start",
        y=["confidence_score_min", "confidence_score_max", "confidence_score_delta"],
        labels={
            "aggregation_start": agg_type,  # X-axis label
            "value": "Confidence Score",   # Y-axis label
            "variable": "Statistics" # Legend title
        },
        markers=True,  
        symbol_sequence=['square'],
        color_discrete_sequence=["#FFFF33", "#00FF00", "#FF8000"] 
    )

    # Add threshold line as a proper trace (enables hover)
    fig2.add_trace(
        go.Scatter(
            x=df_fig2["aggregation_start"],
            y=[0.3]*len(df_fig2),
            mode='lines',
            line=dict(color='red', width=2, dash='dot'),
            name='Target',
        )
    )

    # Update x-axis format for monthly view
    if agg_type == "Monthly":
        fig2.update_xaxes(
            tickformat="%Y-%m",  # Show as "2025-03" format
            dtick="M1"           # One tick per month
        )
    else:  # Weekly
        # Only show labels where data exists
        fig2.update_xaxes(
            tickformat="%Y-%m-%d",
            tickvals=df_fig2["aggregation_start"],  # Only show ticks where data exists
        )

    # Custom hover template 
    fig2.update_traces(
        hovertemplate=(
            "<b>%{customdata[0]}</b><br>" +  # aggregation_label as title
            "<b>%{fullData.name}</b>: %{y:.4f}<extra></extra>"  # Trace name and value
        ),
        customdata=df_fig2[['aggregation_label']]  # Pass the labels as customdata
    )

    # Rename legend entries and customize markers
    fig2.for_each_trace(lambda t: t.update(
        name="Min" if "min" in t.name else "Max" if "max" in t.name else "Delta",
        marker=dict(
            size=8,  # Adjust symbol size
            line=dict(width=1, color='DarkSlateGrey')  # Add border to symbols
        ),
        visible="legendonly" if "min" in t.name or "max" in t.name else True
    ) if t.name != 'Target' else None)

    # Additional styling
    fig2.update_layout(
        hovermode="x unified"
    )
    st.plotly_chart(fig2, use_container_width=True)

    ## --- Plot 3: Histogram of confidence score distribution ---

    st.subheader("Confidence Score Distribution (Min, Max, Mean)")
    df_fig3 = df.copy()

    bin_edges = [0, 0.2, 0.4, 0.6, 0.8, 1.0]
    bin_labels = ["Very Low (0-0.2)", "Low (0.2-0.4)", "Medium (0.4-0.6)", "High (0.6-0.8)", "Very High (0.8-1.0)"]
    metrics = ['Min', 'Max', 'Mean']

    # Bin all metrics
    for name in metrics:
        col = f'confidence_score_{name.lower()}'
        df_fig3[name] = pd.cut(
            df_fig3[col],  
            bins=bin_edges,
            labels=bin_labels,
        )

    # Melt and group
    melted_df = pd.melt(
        df_fig3,
        id_vars=[],
        value_vars=[f"{name}" for name in metrics],
        var_name='metric',
        value_name='binned_confidence'
    )   

    # Group and count
    hist_df = melted_df.groupby(['binned_confidence', 'metric'], observed=True)\
                    .size()\
                    .reset_index(name='count')

    # Create complete combination of all bins and metrics
    complete_combinations = pd.MultiIndex.from_product(
        [bin_labels, metrics],
        names=['binned_confidence', 'metric']
    ).to_frame(index=False)

    # Merge with complete combinations to fill missing values
    hist_merge_df = pd.merge(
        complete_combinations,
        hist_df,
        on=['binned_confidence', 'metric'],
        how='left'
    ).fillna(0)
    
    # Create ordered categorical
    hist_merge_df['binned_confidence'] = pd.Categorical(
        hist_merge_df['binned_confidence'],
        categories=bin_labels,
        ordered=True
    )
        
    fig3 = px.bar(
        hist_merge_df,
        x="binned_confidence",
        y="count",
        color="metric",
        barmode="group",
        category_orders={"binned_confidence": bin_labels},
        labels={
            "binned_confidence": "Confidence Score Range",
            "count": "Count",
            "metric": "Metric Type"
        },
        color_discrete_sequence=['#1f77b4', '#ff7f0e', '#2ca02c']  # Blue, Orange, Green
    )

    # Custom hover template 
    fig3.update_traces(
        hovertemplate=(
            "<b>%{fullData.name}</b>: %{y:.d}<extra></extra>"  # Trace name and value
        ),
        customdata=hist_df[['metric']]  # Pass the labels as customdata
    )

    # Rename legend entries and customize markers
    fig3.for_each_trace(lambda t: t.update(
        visible="legendonly" if "Min" in t.name or "Max" in t.name else True
    ))

    # Improve layout
    fig3.update_layout(
        hovermode="x unified"
    )
    st.plotly_chart(fig3, use_container_width=True)

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
    df2 = fetch_bq_data(query)

    if agg_type == "Monthly":
        df2 = aggregate_monthly(df2)

    ## --- Plot 1: Inference Time Trend ---
    st.subheader(f"{agg_type} Inference Time Trend")

    fig4 = px.line(
        df2,
        x="aggregation_start",
        y=["inference_time_min", "inference_time_med", "inference_time_mean", "inference_time_max"],
        labels={
            "aggregation_start": agg_type,  # X-axis label
            "value": "Inference Time (s)",   # Y-axis label
            "variable": "Statistics" # Legend title
        },
        markers=True,  
        symbol_sequence=['square'],
        color_discrete_sequence=["#0080FF", "#00FFFF", "#0000FF", "#9933FF"]
    )

    # Add threshold line as a proper trace (enables hover)
    fig4.add_trace(
        go.Scatter(
            x=df2["aggregation_start"],
            y=[0.05]*len(df2),
            mode='lines',
            line=dict(color='red', width=2, dash='dot'),
            name='Target',
        )
    )

    # Update x-axis format for monthly view
    if agg_type == "Monthly":
        fig4.update_xaxes(
            tickformat="%Y-%m",  # Show as "2025-03" format
            dtick="M1"           # One tick per month
        )
    else:  # Weekly
        # Only show labels where data exists
        fig4.update_xaxes(
            tickformat="%Y-%m-%d",
            tickvals=df2["aggregation_start"],  # Only show ticks where data exists
        )

    # Custom hover template 
    fig4.update_traces(
        hovertemplate=(
            "<b>%{customdata[0]}</b><br>" +  # aggregation_label as title
            "<b>%{fullData.name}</b>: %{y:.4f}<extra></extra>"  # Trace name and value
        ),
        customdata=df2[['aggregation_label']]  # Pass the labels as customdata
    )

    # Rename legend entries and customize markers
    fig4.for_each_trace(lambda t: t.update(
        name="Median" if "med" in t.name else "Mean" if "mean" in t.name \
                    else "Max" if "max" in t.name else "Min",
        marker=dict(
            size=6,  # Adjust symbol size
            line=dict(width=1, color='DarkSlateGrey')  # Add border to symbols
        ),
        visible="legendonly" if "max" in t.name else True
    ) if t.name != 'Target' else None)

    # Additional styling
    fig4.update_layout(
        hovermode="x unified"
    )

    st.plotly_chart(fig4, use_container_width=True)

    ## --- Plot 2: Histogram of Inference Time Distribution ---

    st.subheader("Inference Time Distribution (Mean, Min, Max)")

    df_fig5 = df2.copy()

    bin_edges2 = [0, 0.05, 0.1, 10000]
    bin_labels2 = ["Fast (0-0.05)", "Decent (0.05-0.1)", "Slow (> 0.1)"]
    metrics2 = ['Mean', 'Min', 'Max']

    # Bin all metrics
    for name in metrics:
        col = f'inference_time_{name.lower()}'
        df_fig5[name] = pd.cut(
            df_fig5[col],  
            bins=bin_edges2,
            labels=bin_labels2,
        )

    # Melt and group
    melted_df2 = pd.melt(
        df_fig5,
        id_vars=[],
        value_vars=[f"{name}" for name in metrics2],
        var_name='metric',
        value_name='binned_inference'
    )   

    # Group and count
    hist_df2 = melted_df2.groupby(['binned_inference', 'metric'], observed=True)\
                    .size()\
                    .reset_index(name='count')

    # Create complete combination of all bins and metrics
    complete_combinations2 = pd.MultiIndex.from_product(
        [bin_labels2, metrics2],
        names=['binned_inference', 'metric']
    ).to_frame(index=False)

    # Merge with complete combinations to fill missing values
    hist_merge_df2 = pd.merge(
        complete_combinations2,
        hist_df2,
        on=['binned_inference', 'metric'],
        how='left'
    ).fillna(0)
    
    # Create ordered categorical
    hist_merge_df2['binned_inference'] = pd.Categorical(
        hist_merge_df2['binned_inference'],
        categories=bin_labels2,
        ordered=True
    )

    fig5 = px.bar(
        hist_merge_df2,
        x="binned_inference",
        y="count",
        color="metric",
        barmode="group",
        category_orders={"binned_inference": bin_labels2},
        labels={
            "binned_inference": "Inference Time Range (s)",
            "count": "Count",
            "metric": "Metric Type"
        },
        color_discrete_sequence=['#2ca02c', '#1f77b4', '#ff7f0e']  # Blue, Orange, Green
    )

    # Custom hover template 
    fig5.update_traces(
        hovertemplate=(
            "<b>%{fullData.name}</b>: %{y:.d}<extra></extra>"  # Trace name and value
        ),
        customdata=hist_df2[['metric']]  # Pass the labels as customdata
    )

    # Rename legend entries and customize markers
    fig5.for_each_trace(lambda t: t.update(
        visible="legendonly" if "Min" in t.name or "Max" in t.name else True
    ))

    # Improve layout
    fig5.update_layout(
        hovermode="x unified"
    )
    st.plotly_chart(fig5, use_container_width=True)

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
