import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
import altair as alt
from datetime import datetime, timezone, timedelta
from google.cloud import bigquery

from google.cloud import bigquery
import google.auth

# ------------------------
# 🔐 Load credentials
# ------------------------

credentials, project_id = google.auth.default()

bq_client = bigquery.Client(credentials=credentials, project=project_id)

# ------------------------
# 📥 Fetch BigQuery data
# ------------------------
@st.cache_data(ttl=600)
def fetch_event_data():
    query = """
    SELECT 
      res_id AS `Result ID`, 
      res_insert_datetime AS `Date`,
      CASE pred_class
        WHEN 'OK' THEN 'Defect Free'
        WHEN 'Defect' THEN 'Fault Detected'
        ELSE 'Inspection Required'
      END AS `Result Label`,
      pred_confidence AS `Confidence Score`
    FROM `cast-defect-detection.cast_defect_detection.inference_results`
    ORDER BY `Date`
    """
    df = bq_client.query(query).to_dataframe()
    df["Date"] = pd.to_datetime(df["Date"])
    return df

# ------------------------
# 🚀 Main UI
# ------------------------
st.title("🧱 Cast Defect Detection Dashboard")

df_events = fetch_event_data()

# 🗓️ Date filtering
st.subheader("📅 Filter Events by Date")
col1, col2 = st.columns(2)
min_date = df_events["Date"].min().date()
max_date = df_events["Date"].max().date()

with col1:
    start_date = st.date_input("Start Date", min_value=min_date, max_value=max_date, value=min_date)
with col2:
    end_date = st.date_input("End Date", min_value=min_date, max_value=max_date, value=max_date)

mask = (df_events["Date"].dt.date >= start_date) & (df_events["Date"].dt.date <= end_date)
filtered_events = df_events[mask].reset_index(drop=True).fillna("")

# 📊 Metrics
st.markdown("### 📈 Metrics Overview")
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Total Inspections", len(filtered_events))
with col2:
    st.metric("Defect Free", (filtered_events["Result Label"] == "Defect Free").sum())
with col3:
    st.metric("Fault Detected", (filtered_events["Result Label"] == "Fault Detected").sum())

# 📈 Trend Chart
st.subheader("📊 Trend Over Time")
chart_data = (
    filtered_events.groupby([filtered_events["Date"].dt.date, "Result Label"])
    .size()
    .reset_index(name="Count")
)

chart = alt.Chart(chart_data).mark_line(point=True).encode(
    x=alt.X("Date:T", title="Date"),
    y=alt.Y("Count:Q", title="Count"),
    color="Result Label:N",
    tooltip=["Date", "Result Label", "Count"]
).properties(width="container", height=300)

st.altair_chart(chart, use_container_width=True)

# 📝 Event Table
st.markdown("### 📝 Event List")
with st.container():
    gb = GridOptionsBuilder.from_dataframe(filtered_events)
    gb.configure_selection('single', use_checkbox=True)
    grid_options = gb.build()

    grid_response = AgGrid(
        filtered_events,
        gridOptions=grid_options,
        height=400,
        theme="material",
        fit_columns_on_grid_load=True,
        allow_unsafe_jscode=True,
        update_mode=GridUpdateMode.SELECTION_CHANGED,
    )

# 🖼️ Image Preview + 💬 Comment
selected_rows = grid_response.get("selected_rows", [])

# Ensure it's a list of dicts
if isinstance(selected_rows, pd.DataFrame):
    selected_rows = selected_rows.to_dict(orient="records")

if isinstance(selected_rows, list) and len(selected_rows) > 0:
    selected = selected_rows[0]
    image_url = selected.get("image_url", "")

    # 🔧 Convert relative GCS path to public URL if needed
    if image_url and not image_url.startswith("http"):
        image_url = f"https://storage.googleapis.com/metal_casting_images/{image_url.lstrip('/')}"

    st.markdown("### 🖼️ Selected Image")
    if image_url:
        st.image(image_url, caption=f"Result ID: {selected['Result ID']}", width=400)
        
    else:
        st.warning("⚠️ No image URL found for this record.")

    # 💬 Comment box
    st.markdown("### 💬 Add a Comment")
    comment = st.text_area("Your comment:", key="comment_box")
    # Create a timezone for Singapore Time (SGT, UTC+8)
    sgt_timezone = timezone(timedelta(hours=8))
    if st.button("Submit Comment"):
        if comment.strip() == "":
            st.warning("Please enter a comment before submitting.")
        else:
            try:
               # Use Singapore Time (SGT) for the timestamp
                timestamp = datetime.now(sgt_timezone)  # Get a timezone-aware datetime object in SGT
                comment_query = f"""
                INSERT INTO `cast-defect-detection.cast_defect_detection.comments` 
                (result_id, comment_text, comment_datetime)
                VALUES (@result_id, @comment, @created_at)
                """
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("result_id", "STRING", selected["Result ID"]),
                        bigquery.ScalarQueryParameter("comment", "STRING", comment),
                        bigquery.ScalarQueryParameter("created_at", "TIMESTAMP", timestamp),
                    ]
                )

                # Execute the query
                query_job = bq_client.query(comment_query, job_config=job_config)
                query_job.result()  # Wait for the query to finish

                st.success("✅ Comment submitted successfully!")

            except Exception as e:
                st.error(f"❌ Error submitting comment: {e}")
                st.text("Detailed Error Message: ")
                st.text(str(e))  # Log the error message to help debug
  