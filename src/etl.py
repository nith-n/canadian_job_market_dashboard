from pathlib import Path
import pandas as pd

RAW_PATH = Path("data/raw/dataset.csv")
OUTPUT_DIR = Path("data/processed")
CLEAN_PATH  = OUTPUT_DIR / "cleaned_dataset.parquet"

COLS = ["year", "month", "month_name", "job_title", "job_type", "experience_level", "salary_min_cad", "salary_max_cad", "salary_median_cad", "number_of_openings", "city", "remote_availability"]

SPECIFIED_CITY = "Halifax"
SPECIFIED_EXPERIENCE_LEVEL = "Entry"

def load_raw() -> pd.DataFrame:
    if not RAW_PATH.exists():
        raise FileNotFoundError(f"Raw Data File not Found at {RAW_PATH}")
    else:
        return pd.read_csv(RAW_PATH)
    
def clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df[COLS].copy()
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
    df = df[df["experience_level"] == SPECIFIED_EXPERIENCE_LEVEL]
    df = df[df["city"] == SPECIFIED_CITY]
    df = df[df["job_title"].isin(["Business Analyst", "Data Scientist"])]
    return df

def complete_time_series(df: pd.DataFrame) -> pd.DataFrame:
    agg = (
        df
        .groupby(
            ["month", "month_name", "job_title", "job_type"],
            as_index=False
        )["number_of_openings"]
        .sum()
    )

    months = (
        agg[["month", "month_name"]]
        .drop_duplicates()
        .sort_values("month")
        .set_index("month")
    )

    job_titles = agg["job_title"].unique()
    job_types = agg["job_type"].unique()

    full_index = pd.MultiIndex.from_product(
        [months.index, job_titles, job_types],
        names=["month", "job_title", "job_type"]
    )

    complete = (
        agg
        .set_index(["month", "job_title", "job_type"])
        .reindex(full_index, fill_value=0)
        .reset_index()
        .merge(
            months.reset_index(),
            on="month",
            how="left",
            validate="many_to_one"  # safety
        )
    )

    # Keep only ONE month_name column
    complete = complete.drop(columns=["month_name_x"]).rename(
        columns={"month_name_y": "month_name"}
    )

    # Final safety check
    assert not complete.duplicated(
        ["month", "job_title", "job_type"]
    ).any()

    return complete

def write_output(df_clean: pd.DataFrame) -> None:

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df_clean.to_parquet(CLEAN_PATH, index=False)
    
    print(f"Clean Data with {len(df_clean):,} Rows Written to {CLEAN_PATH}")

def main():
    df_raw = load_raw()
    df_clean = clean(df_raw)
    df_complete_time_series = complete_time_series(df_clean)
    write_output(df_complete_time_series)

if __name__ == "__main__":
    main()