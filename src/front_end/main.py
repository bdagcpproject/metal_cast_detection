import streamlit as st

def main():
    # Setup
    event_lists = st.Page("event_list.py", title="Event List", icon="🗒️")
    metric_dashboards = st.Page("metric_dashboards.py", title="Metrics Dashboard", icon="📊")

    st.set_page_config(page_title="Metal Casting Defect Detection", 
                       layout="wide")
    pg = st.navigation([event_lists, metric_dashboards])
    pg.run()

if __name__ == "__main__":
    main()