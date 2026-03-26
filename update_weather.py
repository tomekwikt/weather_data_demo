import argparse
import logging
from datetime import date, timedelta

import pandas as pd

from weather_pipeline import (
    DEFAULT_DATA_PATH,
    combine_weekly_data,
    daily_to_weekly,
    enforce_single_forecast_week,
    fetch_daily_weather,
    get_last_tuesday,
    load_weekly_data,
    next_forecast_window,
    normalize_weekly_schema,
    save_data,
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv-path", default=DEFAULT_DATA_PATH)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

    existing = normalize_weekly_schema(load_weekly_data(args.csv_path))

    if existing.empty:
        raise ValueError("No existing weekly dataset found. Run backfill_weather.py first.")

    latest_observed_week_end = pd.Timestamp(get_last_tuesday(date.today() - timedelta(days=1)))
    observed_existing = existing.loc[existing["data_type"] == "observed"]

    if observed_existing.empty:
        raise ValueError("Existing dataset has no observed rows. Run backfill_weather.py first.")

    start_date = (observed_existing["week_end"].max() + pd.Timedelta(days=1)).date()

    if start_date <= latest_observed_week_end.date():
        observed_daily = fetch_daily_weather(start_date.isoformat(), latest_observed_week_end.date().isoformat(), source="observed")
        observed_weekly = daily_to_weekly(observed_daily, data_type="observed")
    else:
        logging.info("Observed weather dataset is already up to date through %s.", latest_observed_week_end.date())
        observed_weekly = pd.DataFrame(columns=existing.columns)

    # Rolling logic:
    # 1. Add newly completed observed weeks from the archive endpoint.
    # 2. Rebuild exactly one next week forecast after the latest observed Tuesday.
    # 3. The merge helper keeps one row per state-week and prefers observed over forecast,
    #    so last week's forecast is automatically replaced once the observation arrives.
    forecast_start, forecast_end = next_forecast_window(latest_observed_week_end)
    forecast_daily = fetch_daily_weather(forecast_start.isoformat(), forecast_end.isoformat(), source="forecast")
    forecast_weekly = daily_to_weekly(forecast_daily, data_type="forecast")

    updated = combine_weekly_data(existing, observed_weekly)
    updated = combine_weekly_data(updated, forecast_weekly)
    updated = enforce_single_forecast_week(updated, forecast_start, forecast_end)
    save_data(updated, args.csv_path)


if __name__ == "__main__":
    main()
