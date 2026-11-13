
import os
import math
import streamlit as st
import pandas as pd
import plotly.express as px
from deltalake import DeltaTable

st.set_page_config(
    page_title="Earthquake Monitor",
    page_icon="🌍",
    layout="wide"
)

# ---------------- CONFIGURATION ----------------

st.title("🌍 Real-Time Earthquake & Aftershock Monitor")
st.caption(
    "Academic demonstration using USGS earthquake data. "
    "Candidate aftershock clusters are heuristic, not official classifications."
)

endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
bucket = os.getenv("MINIO_BUCKET", "earthquake-data")
access = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
secret = os.getenv("MINIO_SECRET_KEY", "minioadmin")

storage_options = {
    "AWS_ENDPOINT_URL": endpoint,
    "AWS_ACCESS_KEY_ID": access,
    "AWS_SECRET_ACCESS_KEY": secret,
    "AWS_REGION": "us-east-1",
    "AWS_ALLOW_HTTP": "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}

GOLD_ROOT = f"s3://{bucket}/lakehouse/gold"
SILVER_ROOT = f"s3://{bucket}/lakehouse/silver"


# ---------------- DATA LOADING ----------------

def load_delta(uri):
    try:
        return DeltaTable(
            uri,
            storage_options=storage_options
        ).to_pandas()
    except Exception as exc:
        st.warning(f"Could not load {uri}: {exc}")
        return pd.DataFrame()


def dashboard():

    silver = load_delta(f"{SILVER_ROOT}/earthquakes")
    clusters = load_delta(f"{GOLD_ROOT}/clusters")
    members = load_delta(f"{GOLD_ROOT}/cluster_events")

    if silver.empty:
        st.error("No Silver earthquake data available.")
        st.info("Check that Kafka, ingestion and Spark are running.")
        return

    # ---------------- CLEAN DATA ----------------

    silver["event_time"] = pd.to_datetime(
        silver["event_time"], utc=True, errors="coerce"
    )

    silver["magnitude"] = pd.to_numeric(
        silver["magnitude"], errors="coerce"
    )

    silver["latitude"] = pd.to_numeric(
        silver["latitude"], errors="coerce"
    )

    silver["longitude"] = pd.to_numeric(
        silver["longitude"], errors="coerce"
    )

    silver = silver.dropna(
        subset=["latitude", "longitude", "magnitude", "event_time"]
    )

    if silver.empty:
        st.warning("No valid earthquake records found.")
        return

    # ---------------- SIDEBAR FILTERS ----------------

    st.sidebar.header("🔍 Filters")

    min_mag = 0.0
    max_mag = max(
        10.0,
        math.ceil(float(silver["magnitude"].max()) * 10) / 10
    )

    mag_range = st.sidebar.slider(
        "Magnitude range",
        min_value=min_mag,
        max_value=max_mag,
        value=(min_mag, max_mag),
        step=0.1
    )

    min_date = silver["event_time"].min().date()
    max_date = silver["event_time"].max().date()

    date_range = st.sidebar.date_input(
        "Event date range",
        value=(min_date, max_date)
    )

    event_types = sorted(
        silver["event_type"].dropna().unique().tolist()
    ) if "event_type" in silver.columns else []

    selected_types = st.sidebar.multiselect(
        "Event type",
        event_types,
        default=event_types
    )

    location_search = st.sidebar.text_input(
        "Search location",
        placeholder="e.g. California, Japan"
    )

    # ---------------- APPLY FILTERS ----------------

    filtered = silver[
        silver["magnitude"].between(*mag_range)
    ].copy()

    if len(date_range) == 2:
        start_date, end_date = date_range

        filtered = filtered[
            (filtered["event_time"].dt.date >= start_date)
            & (filtered["event_time"].dt.date <= end_date)
        ]

    if "event_type" in filtered.columns:
        filtered = filtered[
            filtered["event_type"].isin(selected_types)
        ]

    if location_search:
        filtered = filtered[
            filtered["place"].fillna("").str.contains(
                location_search, case=False, regex=False
            )
        ]

    # ---------------- HEADER METRICS ----------------

    st.subheader("📊 Earthquake Overview")

    total_events = len(filtered)

    strongest = (
        filtered["magnitude"].max()
        if not filtered.empty else 0
    )

    total_clusters = len(clusters)

    total_aftershocks = (
        int(clusters["potential_aftershock_count"].sum())
        if not clusters.empty else 0
    )

    total_cluster_events = (
        int(clusters["event_count"].sum())
        if not clusters.empty else 0
    )

    c1, c2, c3, c4, c5 = st.columns(5)

    c1.metric("Earthquakes", total_events)
    c2.metric("Strongest magnitude", f"{strongest:.1f}")
    c3.metric("Candidate clusters", total_clusters)
    c4.metric("Potential aftershocks", total_aftershocks)
    c5.metric("Events in clusters", total_cluster_events)

    st.divider()

    # ---------------- EARTHQUAKE MAP ----------------

    st.subheader("🌍 Global Earthquake Map")

    if filtered.empty:
        st.info("No earthquakes match the selected filters.")
    else:
        fig = px.scatter_geo(
            filtered,
            lat="latitude",
            lon="longitude",
            color="magnitude",
            size="magnitude",
            hover_name="place",
            hover_data={
                "magnitude": ":.2f",
                "depth": True,
                "event_time": True,
                "latitude": False,
                "longitude": False
            },
            color_continuous_scale="YlOrRd",
            projection="natural earth",
            title="Earthquake locations and magnitudes"
        )

        fig.update_layout(
            height=550,
            margin=dict(l=0, r=0, t=45, b=0)
        )

        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ---------------- ANALYTICS ----------------

    st.subheader("📈 Seismic Activity Analytics")

    left, right = st.columns(2)

    with left:
        st.markdown("**Magnitude Distribution**")

        fig = px.histogram(
            filtered,
            x="magnitude",
            nbins=20,
            color_discrete_sequence=["#E76F51"],
            title="Earthquakes by magnitude"
        )

        st.plotly_chart(fig, use_container_width=True)

    with right:
        st.markdown("**Events Over Time**")

        daily = (
            filtered.assign(
                date=filtered["event_time"].dt.date
            )
            .groupby("date")
            .size()
            .reset_index(name="earthquake_count")
        )

        fig = px.line(
            daily,
            x="date",
            y="earthquake_count",
            markers=True,
            title="Daily earthquake activity"
        )

        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ---------------- CANDIDATE CLUSTERS ----------------

    st.subheader("🔴 Candidate Aftershock Clusters")

    if clusters.empty:
        st.info("No candidate clusters available.")
    else:
        cluster_display = clusters.copy()

        if not members.empty:
            selected_members = members[
                members["event_id"].isin(filtered["id"])
            ].copy()

            selected_members["event_role"] = selected_members.apply(
                lambda row: (
                    "Candidate mainshock"
                    if row["event_id"] == row["cluster_id"]
                    else "Potential aftershock"
                ),
                axis=1
            )

            if not selected_members.empty:
                fig = px.scatter_geo(
                    selected_members,
                    lat="latitude",
                    lon="longitude",
                    color="event_role",
                    symbol="event_role",
                    size="magnitude",
                    hover_name="place",
                    hover_data=[
                        "magnitude",
                        "depth",
                        "cluster_id"
                    ],
                    title="Mainshocks and potential aftershocks",
                    projection="natural earth"
                )

                fig.update_layout(height=500)
                st.plotly_chart(
                    fig,
                    use_container_width=True
                )
            else:
                st.info(
                    "No cluster events match the current filters."
                )

        st.markdown("**Cluster Summary**")

        st.dataframe(
            cluster_display,
            use_container_width=True,
            hide_index=True
        )

        st.markdown("**Cluster Event Membership**")

        if not members.empty:
            st.dataframe(
                members,
                use_container_width=True,
                hide_index=True
            )

    st.divider()

    # ---------------- RAW EVENT DATA ----------------

    with st.expander("📋 View filtered earthquake records"):
        st.dataframe(
            filtered.sort_values(
                "event_time", ascending=False
            ),
            use_container_width=True,
            hide_index=True
        )

    st.caption(
        "Data source: USGS earthquake feed | "
        "Storage: MinIO + Delta Lake | "
        "Processing: Kafka + Spark Structured Streaming"
    )


dashboard()
