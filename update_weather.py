import argparse
import logging
from datetime import date, timedelta

import pandas as pd

from weather_pipeline import (
    DEFAULT_DATA_PATH,
    DEFAULT_FORECAST_CSV,
    DEFAULT_FORECAST_PARQUET,
    DEFAULT_PARQUET_PATH,
    combine_weekly_data,
    daily_to_weekly,
    fetch_daily_weather,
    fetch_forecast_weekly,
    get_last_tuesday,
    load_weekly_data,
    save_data,
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv-path", default=DEFAULT_DATA_PATH)
    parser.add_argument("--parquet-path", default=DEFAULT_PARQUET_PATH)
    parser.add_argument("--forecast-csv-path", default=DEFAULT_FORECAST_CSV)
    parser.add_argument("--forecast-parquet-path", default=DEFAULT_FORECAST_PARQUET)
    parser.add_argument("--forecast-weeks", type=int, default=0)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

    old_weekly = load_weekly_data(args.csv_path, args.parquet_path)

    if old_weekly.empty:
        raise ValueError("No existing weekly dataset found. Run backfill_weather.py first.")

    old_weekly["week_end"] = pd.to_datetime(old_weekly["week_end"])
    start_date = (old_weekly["week_end"].max() + pd.Timedelta(days=1)).date()
    end_date = get_last_tuesday(date.today() - timedelta(days=1))

    if start_date <= end_date:
        daily = fetch_daily_weather(start_date.isoformat(), end_date.isoformat(), source="archive")
        new_weekly = daily_to_weekly(daily)
        full_weekly = combine_weekly_data(old_weekly, new_weekly)
        save_data(full_weekly, args.csv_path, args.parquet_path)
    else:
        logging.info("Observed weather dataset is already up to date.")

    if args.forecast_weeks > 0:
        forecast_weekly = fetch_forecast_weekly(args.forecast_weeks)
        save_data(forecast_weekly, args.forecast_csv_path, args.forecast_parquet_path)


if __name__ == "__main__":
    main()
