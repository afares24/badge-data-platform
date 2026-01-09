import pandas as pd
import streamlit as st
import plotly.express as px
from pipeline import (
    access_distribution,
    premature_badge_outs,
    compliance_distribution
)


TTL_CACHE_DURATION = 10

# --- Page Config --- #
st.set_page_config(
    page_title="Badge Analytics",
    layout="wide"
)

# --- CSS --- #
st.markdown("""
<style>
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
    h1 {
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)



# --- Header --- #
st.title("Badge Analytics")
st.caption("Employee badge-in/out monitoring dashboard")

# --- Load Data --- #
@st.cache_data(ttl=TTL_CACHE_DURATION)
def load_data():
    access_data, access_qs = access_distribution()
    premature_data, premature_qs = premature_badge_outs()
    compliance_data, compliance_qs = compliance_distribution()
    return {
        "access": access_data.df(),
        "premature": premature_data.df(),
        "compliance": compliance_data.df(),
        "query_results": (access_qs,premature_qs,compliance_qs)
    }

data = load_data()

# --- Metrics Row --- #
access_df = data["access"]
compliance_df = data["compliance"]

total_logs = access_df["cnts"].sum()
denied_count = access_df[access_df["access_granted"] == False]["cnts"].sum()
compliant_count = compliance_df[compliance_df["compliance_type"] == "compliant"]["cnts"].sum()
compliance_rate = (compliant_count / total_logs * 100) if total_logs > 0 else 0

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Logs", f"{total_logs:,}")
col2.metric("Access Denied", f"{denied_count:,}")
col3.metric("Compliant Sessions", f"{compliant_count:,}")
col4.metric("Compliance Rate", f"{compliance_rate:.1f}%")

st.divider()

# --- Charts Row --- #
left, right = st.columns(2)

with left:
    st.subheader("Access Distribution")
    fig_access = px.pie(
        access_df,
        values="cnts",
        names=access_df["access_granted"].map({True: "Granted", False: "Denied"}),
        color_discrete_sequence=["#10b981", "#ef4444"],
        hole=0.4
    )
    fig_access.update_layout(margin=dict(t=20, b=20, l=20, r=20))
    st.plotly_chart(fig_access, use_container_width=True)

with right:
    st.subheader("Compliance Distribution")
    fig_compliance = px.pie(
        compliance_df,
        values="cnts",
        names="compliance_type",
        color="compliance_type",
        color_discrete_map={"compliant": "#10b981", "non-compliant": "#f59e0b"},
        hole=0.4
    )
    fig_compliance.update_layout(margin=dict(t=20, b=20, l=20, r=20))
    st.plotly_chart(fig_compliance, use_container_width=True)

st.divider()

# --- Premature Badge Outs Table --- #
st.subheader("Premature Badge-Outs by Department")
st.caption("Sessions under 4 hours")

premature_df = data["premature"]

fig_bar = px.bar(
    premature_df.head(10),
    x="cnts",
    y="dept",
    color="building_location",
    orientation="h",
    color_discrete_sequence=px.colors.qualitative.Set2
)
fig_bar.update_layout(
    margin=dict(t=20, b=20, l=20, r=20),
    yaxis_title="",
    xaxis_title="Count",
    legend_title="Building"
)
st.plotly_chart(fig_bar, use_container_width=True)

# --- Expander for Raw Data --- #
with st.expander("View Raw Data"):
    tab1, tab2, tab3 = st.tabs(["Access", "Compliance", "Premature Outs"])
    with tab1:
        st.dataframe(access_df, use_container_width=True)
    with tab2:
        st.dataframe(compliance_df, use_container_width=True)
    with tab3:
        st.dataframe(premature_df, use_container_width=True)

# --- Refresh Button --- #
st.divider()
col_left, col_right = st.columns([3, 1])
with col_right:
    if st.button("Refresh Data"):
        st.cache_data.clear()
        st.rerun()


# --- Query Result Monitoring --- #
st.divider()
st.subheader("Query Performance")


access_qs, premature_qs, compliance_qs = data["query_results"]

qs_df = pd.DataFrame(
    [access_qs, premature_qs, compliance_qs],
    columns=["process", "elapsed_time"]
)
qs_df["elapsed_time_ms"] = qs_df["elapsed_time"] * 1000  # converts to ms

# Bar charts
fig_qs = px.bar(
    qs_df,
    x="process",
    y="elapsed_time_ms",
    color="process",
    color_discrete_sequence=["#6366f1", "#8b5cf6", "#a855f7"]
)
fig_qs.update_layout(
    margin=dict(t=20, b=20, l=20, r=20),
    xaxis_title="",
    yaxis_title="Elapsed Time (ms)",
    showlegend=False
)
st.plotly_chart(fig_qs, use_container_width=True)

col1, col2, col3 = st.columns(3)
col1.metric("access_distribution", f"{access_qs[1]*1000:.2f} ms")
col2.metric("premature_badge_outs", f"{premature_qs[1]*1000:.2f} ms")
col3.metric("compliance_distribution", f"{compliance_qs[1]*1000:.2f} ms")
