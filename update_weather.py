import argparse
import logging
from datetime import date, timedelta

import pandas as pd

from weather_pipeline import (
    DEFAULT_DATA_PATH,
    combine_weekly_data,
    daily_to_weekly,
    fetch_daily_weather,
    get_last_tuesday,
    load_weekly_data,
    save_data,
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv-path", default=DEFAULT_DATA_PATH)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

    old_weekly = load_weekly_data(args.csv_path)

    if old_weekly.empty:
        raise ValueError("No existing weekly dataset found. Run backfill_weather.py first.")

    old_weekly["week_end"] = pd.to_datetime(old_weekly["week_end"])
    start_date = (old_weekly["week_end"].max() + pd.Timedelta(days=1)).date()
    end_date = get_last_tuesday(date.today() - timedelta(days=1))

    if start_date <= end_date:
        daily = fetch_daily_weather(start_date.isoformat(), end_date.isoformat())
        new_weekly = daily_to_weekly(daily)
        full_weekly = combine_weekly_data(old_weekly, new_weekly)
        save_data(full_weekly, args.csv_path)
    else:
        logging.info("Observed weather dataset is already up to date.")


if __name__ == "__main__":
    main()
