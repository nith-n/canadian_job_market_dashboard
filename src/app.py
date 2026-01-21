import streamlit as st
import pandas as pd
import altair as alt

st.set_page_config(page_title="Canadian Job Market Analysis 2025", layout="wide")

st.title("Canadian Job Market Analysis 2025")

# -----------------------------
# Load data
# -----------------------------
df = pd.read_parquet("data/processed/cleaned_dataset.parquet")

# -----------------------------
# Filters
# -----------------------------

# Month filter
months = (
    df[["month", "month_name"]]
    .drop_duplicates()
    .sort_values("month")
)

month_options = ["All"] + months["month_name"].tolist()

selected_month = st.selectbox(
    "Select month (bar chart only)",
    month_options
)

# City filter
cities = sorted(df["city"].dropna().unique().tolist())
cities.insert(0, "All")

selected_city = st.selectbox(
    "Select city",
    cities
)

# -----------------------------
# Apply filters (clean separation)
# -----------------------------

# City filter applies globally
city_df = df.copy()
if selected_city != "All":
    city_df = city_df[city_df["city"] == selected_city]

# Month filter applies ONLY to bar chart
bar_df = city_df.copy()
if selected_month != "All":
    bar_df = bar_df[bar_df["month_name"] == selected_month]

# -----------------------------
# Bar chart: Job titles by openings
# (month + city)
# -----------------------------
openings_by_title_type = (
    bar_df
    .groupby(["job_title", "job_type"], as_index=False)
    ["number_of_openings"]
    .sum()
)

st.subheader("Job Titles by Number of Openings")

bar_chart = (
    alt.Chart(openings_by_title_type)
    .mark_bar()
    .encode(
        x=alt.X(
            "job_title:N",
            title="Job Title",
            axis=alt.Axis(labelAngle=0)
        ),
        y=alt.Y(
            "number_of_openings:Q",
            title="Number of Openings"
        ),
        xOffset=alt.XOffset("job_type:N"),
        color=alt.Color("job_type:N", title="Job Type"),
        tooltip=[
            "job_title",
            "job_type",
            "number_of_openings"
        ]
    )
)

st.altair_chart(bar_chart, use_container_width=True)

# -----------------------------
# Line chart: Openings over time
# (city only, full timeline)
# -----------------------------
openings_over_time = (
    city_df
    .groupby(
        ["month", "month_name", "job_title", "job_type"],
        as_index=False
    )["number_of_openings"]
    .sum()
)

st.subheader("Job Openings Over Time")

line_chart = (
    alt.Chart(openings_over_time)
    .mark_line(point=True)
    .encode(
        x=alt.X(
            "month:Q",
            title="Month",
            axis=alt.Axis(tickMinStep=1)
        ),
        y=alt.Y(
            "number_of_openings:Q",
            title="Number of Openings"
        ),
        color=alt.Color(
            "job_type:N",
            title="Job Type"
        ),
        tooltip=[
            "job_title",
            "job_type",
            "month_name",
            "number_of_openings"
        ]
    )
    .facet(
        facet=alt.Facet(
            "job_title:N",
            title="Job Title"
        ),
        columns=2
    )
)

st.altair_chart(line_chart, use_container_width=True)

# -----------------------------
# Pie charts: Job type share by job title
# (city only)
# -----------------------------

st.subheader("Job Type Distribution by Job Title (%)")

JOB_TITLE_1 = "Business Analyst"
JOB_TITLE_2 = "Data Scientist"


def prepare_pie_data(df, job_title):
    pie_df = (
        df[df["job_title"] == job_title]
        .groupby("job_type", as_index=False)["number_of_openings"]
        .sum()
    )

    total = pie_df["number_of_openings"].sum()
    if total > 0:
        pie_df["percentage"] = (
            pie_df["number_of_openings"] / total * 100
        )

    return pie_df


def job_type_pie(df, title):
    return (
        alt.Chart(df)
        .mark_arc()
        .encode(
            theta=alt.Theta("percentage:Q", stack=True),
            color=alt.Color(
                "job_type:N",
                legend=alt.Legend(title="Job Type")
            ),
            tooltip=[
                alt.Tooltip("job_type:N", title="Job Type"),
                alt.Tooltip("number_of_openings:Q", title="Openings"),
                alt.Tooltip(
                    "percentage:Q",
                    title="Percentage",
                    format=".1f"
                ),
            ],
        )
        .properties(title=title)
    )


pie_df_1 = prepare_pie_data(city_df, JOB_TITLE_1)
pie_df_2 = prepare_pie_data(city_df, JOB_TITLE_2)

col1, col2 = st.columns(2)

with col1:
    if pie_df_1.empty:
        st.info(f"No data available for {JOB_TITLE_1}")
    else:
        st.altair_chart(
            job_type_pie(
                pie_df_1,
                f"{JOB_TITLE_1} – Job Type Share"
            ),
            use_container_width=True
        )

with col2:
    if pie_df_2.empty:
        st.info(f"No data available for {JOB_TITLE_2}")
    else:
        st.altair_chart(
            job_type_pie(
                pie_df_2,
                f"{JOB_TITLE_2} – Job Type Share"
            ),
            use_container_width=True
        )


