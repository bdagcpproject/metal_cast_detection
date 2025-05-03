import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
import pandas as pd
from datetime import datetime, timezone, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account
import altair as alt
import google.auth

# ------------------------
#  Load credentials
# ------------------------
credentials, project_id = google.auth.default()
bq_client = bigquery.Client(credentials=credentials, project=project_id)

# ------------------------
#  Fetch BigQuery data
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
      pred_confidence AS `Confidence Score`,
      res_image_path AS `image_url`
    FROM `cast-defect-detection.cast_defect_detection.inference_results`
    ORDER BY `Date`
    """
    df = bq_client.query(query).to_dataframe()
    df["Date"] = pd.to_datetime(df["Date"])
    return df

# ------------------------
#  Outlier Insights
# ------------------------
@st.cache_data(ttl=600)
def fetch_outlier_insights():
    query = """
    WITH base AS (
      SELECT 
        res_id,
        res_insert_datetime,
        pred_confidence,
        pred_Speed,
        pred_class
      FROM `cast-defect-detection.cast_defect_detection.inference_results`
    ),
    defects_per_day AS (
      SELECT DATE(res_insert_datetime) AS defect_date, COUNT(*) AS defect_count
      FROM base
      WHERE pred_class = 'Defect'
      GROUP BY defect_date
      ORDER BY defect_count DESC
      LIMIT 1
    ),
    lowest_confidence AS (
      SELECT res_id, pred_confidence, res_insert_datetime
      FROM base
      ORDER BY pred_confidence ASC
      LIMIT 1
    ),
    highest_inference_time AS (
      SELECT res_id, pred_Speed, res_insert_datetime
      FROM base
      ORDER BY pred_Speed DESC
      LIMIT 1
    )

    SELECT 
      (SELECT pred_confidence FROM lowest_confidence) AS lowest_confidence_score,
      (SELECT res_insert_datetime FROM lowest_confidence) AS lowest_confidence_date,
      (SELECT pred_Speed FROM highest_inference_time) AS highest_inference_time,
      (SELECT res_insert_datetime FROM highest_inference_time) AS highest_inference_date,
      (SELECT defect_date FROM defects_per_day) AS most_defect_day,
      (SELECT defect_count FROM defects_per_day) AS most_defect_count
    """
    return bq_client.query(query).to_dataframe().iloc[0]

# ------------------------
# 🚀 Main UI
# ------------------------
st.title(" 🗒️Cast Defect Detection Dashboard")

df_events = fetch_event_data()

#  Date filtering
st.sidebar.subheader("📅 Filter by Date")
min_date = df_events["Date"].min().date()
max_date = df_events["Date"].max().date()

start_date = st.sidebar.date_input("Start Date", min_value=min_date, max_value=max_date, value=min_date)
end_date = st.sidebar.date_input("End Date", min_value=min_date, max_value=max_date, value=max_date)


mask = (df_events["Date"].dt.date >= start_date) & (df_events["Date"].dt.date <= end_date)
filtered_events = df_events[mask].reset_index(drop=True).fillna("")

#  Metrics + Outliers
st.markdown("###  Metrics Overview")

try:
    insights = fetch_outlier_insights()
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    with col1:
        st.metric("Total Inspections", len(filtered_events))
    with col2:
        st.metric("Defect Free", (filtered_events["Result Label"] == "Defect Free").sum())
    with col3:
        st.metric("Fault Detected", (filtered_events["Result Label"] == "Fault Detected").sum())
    with col4:
        st.markdown(f"""
        <div style="padding: 10px; background-color: #fff4f4; border-left: 4px solid #dc3545; border-radius: 4px;">
            <div style="font-size: 16px; font-weight: bold;">🔻 Lowest Confidence Score</div>
            <div style="font-size: 20px; color: black;">{insights['lowest_confidence_score']:.2f}</div>
            <div style="font-size: 13px; color: #dc3545;">on {insights['lowest_confidence_date'].date()}</div>
        </div>
    """, unsafe_allow_html=True)

    with col5:
        st.markdown(f"""
        <div style="padding: 10px; background-color: #fff4f4; border-left: 4px solid #dc3545; border-radius: 4px;">
            <div style="font-size: 16px; font-weight: bold;">🔺 Highest Inference Time</div>
            <div style="font-size: 20px; color: black;">{insights['highest_inference_time']} ms</div>
            <div style="font-size: 13px; color: #dc3545;">on {insights['highest_inference_date'].date()}</div>
        </div>
    """, unsafe_allow_html=True)

    with col6:
        st.markdown(f"""
        <div style="padding: 10px; background-color: #fff4f4; border-left: 4px solid #dc3545; border-radius: 4px;">
            <div style="font-size: 16px; font-weight: bold;">🔥 Most Defects (1 Day)</div>
            <div style="font-size: 20px; color: black;">{insights['most_defect_count']} defects</div>
            <div style="font-size: 13px; color: #dc3545;">on {insights['most_defect_day']}</div>
        </div>
    """, unsafe_allow_html=True)

except Exception as e:
    st.error("⚠️ Failed to load outlier insights.")
    st.text(str(e))

#  Trend Chart
st.subheader(" Trend Over Time")
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


st.markdown("###  Prediction Results List")
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

# 💬 Image Preview + Comments
selected_rows = grid_response.get("selected_rows", [])

if isinstance(selected_rows, pd.DataFrame):
    selected_rows = selected_rows.to_dict(orient="records")

if isinstance(selected_rows, list) and len(selected_rows) > 0:
    selected = selected_rows[0]
    result_id = selected["Result ID"]

    image_url = selected.get("image_url", "")
    if image_url and not image_url.startswith("http"):
        image_url = f"https://storage.googleapis.com/metal_casting_images/{image_url.lstrip('/')}"

    st.markdown("### 🖼️ Selected Image")
    if image_url:
        st.image(image_url, caption=f"Result ID: {result_id}", width=400)
    else:
        st.warning("⚠️ No image URL found for this record.")

    # 💬 Existing Comments
    st.markdown("### 💬 Existing Comments")
    try:
        comments_query = """
        SELECT comment_text, comment_datetime
        FROM `cast-defect-detection.cast_defect_detection.comments`
        WHERE result_id = @result_id
        ORDER BY comment_datetime DESC
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("result_id", "STRING", result_id)
            ]
        )
        comments_df = bq_client.query(comments_query, job_config=job_config).to_dataframe()
        if not comments_df.empty:
            for _, row in comments_df.iterrows():
                st.markdown(f"""
                    <div style="padding: 8px 12px; border-left: 4px solid #007BFF; margin-bottom: 10px; background-color: #f9f9f9;">
                        <strong>{row['comment_datetime'].strftime('%Y-%m-%d %H:%M:%S')}</strong><br>
                        {row['comment_text']}
                    </div>
                """, unsafe_allow_html=True)
        else:
            st.info("No comments yet for this result.")
    except Exception as e:
        st.error("Failed to load comments.")
        st.text(str(e))

    st.markdown("### 💬 Add a Comment")
    comment = st.text_area("Your comment:", key="comment_box", height=100)

    # Submit comment
    if st.button("Submit Comment"):
        if not comment.strip():
            st.warning("Please enter a comment before submitting.")
        else:
            try:
                timestamp = datetime.now(timezone(timedelta(hours=8)))
                insert_query = """
                INSERT INTO `cast-defect-detection.cast_defect_detection.comments`
                (result_id, comment_text, comment_datetime)
                VALUES (@result_id, @comment, @created_at)
                """
                insert_job = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("result_id", "STRING", selected["Result ID"]),
                        bigquery.ScalarQueryParameter("comment", "STRING", comment),
                        bigquery.ScalarQueryParameter("created_at", "TIMESTAMP", timestamp),
                    ]
                )
                bq_client.query(insert_query, job_config=insert_job).result()
                st.success("✅ Comment submitted successfully!")
            except Exception as e:
                st.error(f"❌ Error submitting comment: {e}")
                st.text(str(e))
