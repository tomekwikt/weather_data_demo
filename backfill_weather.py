import argparse
import logging

from weather_pipeline import (
    DEFAULT_DATA_PATH,
    DEFAULT_PARQUET_PATH,
    daily_to_weekly,
    fetch_daily_weather,
    save_data,
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--csv-path", default=DEFAULT_DATA_PATH)
    parser.add_argument("--parquet-path", default=DEFAULT_PARQUET_PATH)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

    daily = fetch_daily_weather(args.start_date, args.end_date, source="archive")
    weekly = daily_to_weekly(daily)
    save_data(weekly, args.csv_path, args.parquet_path)


if __name__ == "__main__":
    main()
