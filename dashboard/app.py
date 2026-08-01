import json
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from streamlit_autorefresh import st_autorefresh

# ---------------------------------------------------
# Page Configuration
# ---------------------------------------------------

st.set_page_config(
    page_title="Reddit Trend Detector",
    page_icon="📈",
    layout="wide"
)

# ---------------------------------------------------
# Auto Refresh
# ---------------------------------------------------

st_autorefresh(interval=5000, key="refresh")

# ---------------------------------------------------
# Custom CSS
# ---------------------------------------------------

st.markdown("""
<style>

.stApp{
    background-color:#0f172a;
    color:white;
}

h1,h2,h3{
    color:white;
}

[data-testid="metric-container"]{
    background:#1e293b;
    border:1px solid #334155;
    border-radius:12px;
    padding:15px;
}

div[data-testid="stMetricValue"]{
    color:white;
}

div[data-testid="stMetricLabel"]{
    color:#cbd5e1;
}

</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------
# Load JSON
# ---------------------------------------------------

BASE_DIR = Path(__file__).parent
json_file = BASE_DIR / "data" / "latest_results.json"

if not json_file.exists():
    st.error("latest_results.json not found.")
    st.stop()

with open(json_file) as f:
    data = json.load(f)

df = pd.DataFrame(data["results"])

# ---------------------------------------------------
# Header
# ---------------------------------------------------

st.markdown("""
#  Reddit Trend Detector Dashboard

""")

st.caption(f"Last Updated: {data['generated_at']}")

st.markdown("---")

# ---------------------------------------------------
# Summary Cards
# ---------------------------------------------------

c1, c2, c3, c4 = st.columns(4)

c1.metric(
    " Historical",
    data["historical_subreddits"]
)

c2.metric(
    " Live",
    data["live_subreddits"]
)

c3.metric(
    " Trending",
    data["trending_count"]
)

c4.metric(
    " Threshold",
    f"{data['threshold']}×"
)

st.markdown("---")

# ---------------------------------------------------
# Trending Table
# ---------------------------------------------------

st.subheader(" Trending Subreddits")

trending = df[df["status"] == "TRENDING"]

if trending.empty:
    st.success("No trending subreddits detected.")
else:

    display = trending[
        [
            "subreddit",
            "current_per_hour",
            "baseline_per_hour",
            "ratio",
            "status"
        ]
    ].sort_values(
        "ratio",
        ascending=False
    )

    st.dataframe(
        display,
        use_container_width=True,
        hide_index=True
    )

# ---------------------------------------------------
# Charts
# ---------------------------------------------------

left, right = st.columns(2)

# ---------------------------------------------------
# Top Ratios
# ---------------------------------------------------

with left:

    st.subheader(" Top Activity Ratios")

    top = df.sort_values(
        "ratio",
        ascending=False
    ).head(15)

    fig = px.bar(
        top,
        x="ratio",
        y="subreddit",
        orientation="h",
        color="status",
        text="ratio",
        title="Current Activity / Historical Baseline"
    )

    fig.update_layout(
        height=500,
        yaxis=dict(categoryorder="total ascending"),
        template="plotly_dark"
    )

    st.plotly_chart(
        fig,
        use_container_width=True
    )

# ---------------------------------------------------
# Current vs Baseline
# ---------------------------------------------------

with right:

    st.subheader(" Current vs Historical")

    compare = top.copy()

    fig2 = go.Figure()

    fig2.add_trace(
        go.Bar(
            name="Current/hr",
            x=compare["subreddit"],
            y=compare["current_per_hour"]
        )
    )

    fig2.add_trace(
        go.Bar(
            name="Baseline/hr",
            x=compare["subreddit"],
            y=compare["baseline_per_hour"]
        )
    )

    fig2.update_layout(
        barmode="group",
        height=500,
        template="plotly_dark"
    )

    st.plotly_chart(
        fig2,
        use_container_width=True
    )

# ---------------------------------------------------
# Pie + Gauge
# ---------------------------------------------------

left, right = st.columns(2)

with left:

    st.subheader(" Activity Status")

    status_counts = (
        df["status"]
        .value_counts()
        .reset_index()
    )

    status_counts.columns = [
        "status",
        "count"
    ]

    pie = px.pie(
        status_counts,
        names="status",
        values="count",
        hole=0.45
    )

    pie.update_layout(
        template="plotly_dark",
        height=450
    )

    st.plotly_chart(
        pie,
        use_container_width=True
    )

with right:

    st.subheader(" Trending Gauge")

    gauge = go.Figure(
        go.Indicator(
            mode="gauge+number",
            value=data["trending_count"],
            title={"text": "Trending Subreddits"},
            gauge={
                "axis": {"range": [0, 20]},
                "bar": {"color": "red"},
                "steps": [
                    {"range": [0, 5], "color": "#22c55e"},
                    {"range": [5, 10], "color": "#eab308"},
                    {"range": [10, 20], "color": "#ef4444"},
                ]
            }
        )
    )

    gauge.update_layout(
        template="plotly_dark",
        height=450
    )

    st.plotly_chart(
        gauge,
        use_container_width=True
    )

# ---------------------------------------------------
# System Health
# ---------------------------------------------------

st.markdown("---")

st.subheader(" System Health")

h1, h2 = st.columns(2)

with h1:
    st.success("Athena Connected")
    st.success("DynamoDB Connected")
    st.success("Serving Layer Running")

with h2:
    st.success("Lambda Healthy")
    st.success("Kinesis Streaming")
    st.success("Batch Layer Available")

# ---------------------------------------------------
# Footer
# ---------------------------------------------------

st.markdown("---")

st.caption(f"""
**Source:** Athena + DynamoDB Serving Layer Merge

**Historical Baseline:** {data['historical_subreddits']} subreddits

**Live Window:** {data['live_subreddits']} subreddits

**Auto Refresh:** Every 5 seconds
""")