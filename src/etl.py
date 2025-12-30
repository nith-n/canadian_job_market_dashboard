from pathlib import Path
import pandas as pd

RAW_PATH = Path("data/raw/dataset.csv")
OUTPUT_DIR = Path("data/processed")
CLEAN_PATH  = OUTPUT_DIR / "cleaned_dataset.parquet"
METRICS_PATH = OUTPUT_DIR / "metrics_dataset.txt"

COLS = ["year", 
        "month", 
        "job_title", 
        "industry", 
        "experience_level", 
        "salary_min_cad", 
        "salary_max_cad", 
        "salary_median_cad", 
        "number_of_openings",  
        "hiring_trend", 
        "city", 
        "region", 
        "education_requirement", 
        "job_type", 
        "skills_required", 
        "remote_availability", 
        "salary_range_cad", 
        "salary_variability", 
        "salary_bracket", 
        "salary_log_median", 
        "quarter", 
        "month_name", 
        "is_high_demand", 
        "openings_level", 
        "metro_city", 
        "cost_region", 
        "city_salary_adjusted_index", 
        "job_market_pressure", 
        "competitiveness_score"]

def load_raw() -> pd.DataFrame:
    if not RAW_PATH.exists():
        raise FileNotFoundError(f"Raw Data File not Found at {RAW_PATH}")
    else:
        return pd.read_csv(RAW_PATH)
    
def clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df[COLS].copy()
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
    return df

def write_output(df_clean: pd.DataFrame) -> None:

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df_clean.to_parquet(CLEAN_PATH, index=False)
    
    print(f"Clean Data with {len(df_clean):,} Rows Written to {CLEAN_PATH}")

def main():
    df_raw = load_raw()
    df_clean = clean(df_raw)
    write_output(df_clean)
    
if __name__ == "__main__":
    main()