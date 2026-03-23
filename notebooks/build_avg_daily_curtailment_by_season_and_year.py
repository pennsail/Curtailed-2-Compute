"""
Build avg_daily_curtailment_by_season_and_year.csv for 2020-2025.
Uses same logic as 01_curtailed_energy_analysis.ipynb (Spring = Feb-Jun, Non-Spring = Jan, Jul-Dec).
Run from repo root: python notebooks/build_avg_daily_curtailment_by_season_and_year.py
"""
from pathlib import Path
import pandas as pd

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
OUT_CSV = Path(__file__).resolve().parent / "avg_daily_curtailment_by_season_and_year.csv"
CONVERSION_FACTOR = 5 / 60  # 5 min -> hours
SPRING_MONTHS = [2, 3, 4, 5, 6]
YEARS = [2020, 2021, 2022, 2023, 2024, 2025]

def main():
    combined = []
    for year in YEARS:
        path = DATA_DIR / f"productionandcurtailmentsdata_{year}.xlsx"
        if not path.exists():
            print(f"Skip (not found): {path.name}")
            continue
        try:
            xl = pd.ExcelFile(path)
            df = xl.parse("Curtailments")
            df["Year"] = year
            combined.append(df)
            print(f"Loaded: {path.name}")
        except Exception as e:
            print(f"Error {path.name}: {e}")
            raise

    curtailment_df = pd.concat(combined, ignore_index=True)
    curtailment_df = curtailment_df.copy()
    curtailment_df["Wind Curtailment"] = curtailment_df["Wind Curtailment"].fillna(0)
    curtailment_df["Solar Curtailment"] = curtailment_df["Solar Curtailment"].fillna(0)
    curtailment_df["Total Curtailment (MW)"] = (
        curtailment_df["Solar Curtailment"] + curtailment_df["Wind Curtailment"]
    )
    curtailment_df["Datetime"] = pd.to_datetime(curtailment_df["Date"]) + pd.to_timedelta(
        (curtailment_df["Hour"] - 1) * 60 + (curtailment_df["Interval"] - 1) * 5, unit="m"
    )
    curtailment_df["Total Curtailment (MW) (MWh)"] = (
        curtailment_df["Total Curtailment (MW)"] * CONVERSION_FACTOR
    )
    curtailment_df["Date"] = pd.to_datetime(curtailment_df["Date"])
    # Group by calendar date (normalize so same day is one group)
    curtailment_df["DateOnly"] = curtailment_df["Date"].dt.normalize()
    daily_totals = (
        curtailment_df.groupby("DateOnly")
        .agg(Daily_Curtailment_MWh=("Total Curtailment (MW) (MWh)", "sum"))
        .reset_index()
        .rename(columns={"DateOnly": "Date"})
    )
    daily_totals["Year"] = daily_totals["Date"].dt.year
    daily_totals["Month_Num"] = daily_totals["Date"].dt.month

    spring_df = daily_totals[daily_totals["Month_Num"].isin(SPRING_MONTHS)]
    non_spring_df = daily_totals[~daily_totals["Month_Num"].isin(SPRING_MONTHS)]

    spring_summary = (
        spring_df.groupby("Year")["Daily_Curtailment_MWh"]
        .mean()
        .reset_index(name="Spring_Daily_Avg_MWh")
    )
    non_spring_summary = (
        non_spring_df.groupby("Year")["Daily_Curtailment_MWh"]
        .mean()
        .reset_index(name="NonSpring_Daily_Avg_MWh")
    )
    season_comparison = pd.merge(spring_summary, non_spring_summary, on="Year")
    season_comparison = season_comparison.sort_values("Year").reset_index(drop=True)
    season_comparison.to_csv(OUT_CSV, index=False)
    print(f"Wrote {OUT_CSV}")
    print(season_comparison.to_string(index=False))

if __name__ == "__main__":
    main()
