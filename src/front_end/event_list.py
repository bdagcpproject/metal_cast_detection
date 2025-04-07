import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder
import pandas as pd
from google.cloud import bigquery, storage

# Set up Google Cloud clients
bq_client = bigquery.Client()
gcs_client = storage.Client()

def fetch_event_data():     
    # Example BigQuery query - modify according to your schema
    query = """
        SELECT event_id, event_name, image_url, timestamp 
        FROM your_dataset.events_table
        LIMIT 100
    """
    return bq_client.query(query).to_dataframe()

def fetch_metrics(start_date, end_date):
    # Example metrics query - modify according to your schema
    # query = f"""
    #     SELECT metric_name, value, timestamp
    #     FROM your_dataset.metrics_table
    #     WHERE timestamp BETWEEN '{start_date}' AND '{end_date}'
    # """
    # return bq_client.query(query).to_dataframe()
    df_metrics = {
        "metric_name": ["Metric 1", "Metric 2", "Metric 3", "Metric 4"],
        "value": [10, 20, 30, 40],
        "timestamp": [101, 102, 103, 104],
    }

    return df_metrics.to_dataframe()

@st.dialog("Image Preview")
def show_image_modal():
    if st.session_state.get("selected_image"):
        st.image(st.session_state.selected_image)
        if st.button("Close"):
            # Reset both modal state and grid selection
            #st.session_state.selected_rows = st.session_state.selected_rows.iloc[:0]
            #print(f'post content = {st.session_state.selected_rows}')
            st.session_state.show_modal = False
            st.session_state.selected_image = None
            st.rerun()
    else:
        st.warning("No image selected")

# Event List Page
st.title("Event List")

# Fetch data
#df_events = fetch_event_data()
dict_events = {
    "Event ID": [101, 102, 103, 104],
    "Event Name": ["Inspection Start", "Fault Detected", "Maintenance", "Inspection End"],
    "Date": ["2023-04-01", "2023-04-02", "2023-04-03", "2023-04-04"],
    "Status": ["OK", "Defect", "Scheduled", "OK"],
    "image_url": ["https://storage.googleapis.com/metal_casting_images/result/cast_def_0_1171.jpeg", 
                    "https://storage.googleapis.com/metal_casting_images/result/cast_def_0_1171.jpeg", 
                    "https://storage.googleapis.com/metal_casting_images/result/cast_def_0_1171.jpeg", 
                    "https://storage.googleapis.com/metal_casting_images/result/cast_def_0_1171.jpeg"]
}
df_events = pd.DataFrame.from_dict(dict_events)

# Configure AgGrid
gb = GridOptionsBuilder.from_dataframe(df_events)
gb.configure_selection('single')
grid_options = gb.build()

# Display AgGrid
grid_response = AgGrid(
    df_events,
    gridOptions=grid_options,
    height=400,
    allow_unsafe_jscode=True,
    update_on='selectionChanged',
)
    
# Handle row selection
if not grid_response['selected_rows'] is None and not st.session_state.selected_rows.equals(grid_response['selected_rows']):
    st.session_state.selected_rows = grid_response['selected_rows']
else:
    st.session_state.selected_rows  = pd.DataFrame()    

st.session_state.show_modal = False
print(f'show_modal state {st.session_state.get("show_modal")}')

if not st.session_state.selected_rows is None and len(st.session_state.selected_rows) > 0 and not st.session_state.get("show_modal"):
    image_url = "https://storage.googleapis.com/metal_casting_images/result/cast_def_0_1171.jpeg"
    if image_url:
        st.session_state.selected_image = image_url
        st.session_state.show_modal = True
        show_image_modal() 