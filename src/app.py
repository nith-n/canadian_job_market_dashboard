import streamlit as st
import pandas as pd
import altair as alt

st.title("Canadian Job Market Analysis 2025")

df = pd.read_parquet("data/processed/cleaned_dataset.parquet")

#filters
months = (
    df[["month", "month_name"]]
    .drop_duplicates()
    .sort_values("month")
)

month_options = ["All"] + months["month_name"].tolist()

selected_month = st.selectbox(
    "select month",
    month_options
)

if selected_month == "All":
    filtered_df = df
else:
    filtered_df = df[df["month_name"] == selected_month]

#bar_graph

openings_by_title_type = (
    filtered_df
    .groupby(["job_title", "job_type"], as_index = False)["number_of_openings"]
    .sum()
)

st.subheader("Job Titles by Number of Openings")

chart = (
    alt.Chart(openings_by_title_type)
    .mark_bar()
    .encode(
        x=alt.X("job_title:N", title="Job Title", axis=alt.Axis(labelAngle=0)),
        y=alt.Y("number_of_openings:Q", title="Number of Openings"),
        xOffset=alt.XOffset("job_type:N"),
        color=alt.Color("job_type:N", title="Job Type"),
        tooltip=["job_title", "job_type", "number_of_openings"]
    )
)

st.altair_chart(chart, use_container_width=True)

#line_graph

openings_over_time = df.copy()

chart = (
    alt.Chart(openings_over_time)
    .mark_line(point=True)
    .encode(
        x=alt.X(
            "month:Q",
            title="Month",
            axis=alt.Axis(
                tickMinStep=1,
                labelExpr="datum.value == 1 ? 'Jan' : "
                          "datum.value == 2 ? 'Feb' : "
                          "datum.value == 3 ? 'Mar' : "
                          "datum.value == 4 ? 'Apr' : "
                          "datum.value == 5 ? 'May' : "
                          "datum.value == 6 ? 'Jun' : "
                          "datum.value == 7 ? 'Jul' : "
                          "datum.value == 8 ? 'Aug' : "
                          "datum.value == 9 ? 'Sep' : "
                          "datum.value == 10 ? 'Oct' : "
                          "datum.value == 11 ? 'Nov' : 'Dec'"
            )
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

st.altair_chart(chart, use_container_width=True)

