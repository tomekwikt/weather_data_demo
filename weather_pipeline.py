import logging
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests


logger = logging.getLogger(__name__)

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
DAILY_VARS = "temperature_2m_mean,precipitation_sum,relative_humidity_2m_mean"
DEFAULT_DATA_PATH = "data/weather_state_weekly.csv"
MAX_RETRIES = 3
REQUEST_DELAY_SECONDS = 2
RATE_LIMIT_BACKOFF_SECONDS = 30


STATES = [
    {"state": "Alabama", "state_abbr": "AL", "latitude": 32.806671, "longitude": -86.791130},
    {"state": "Alaska", "state_abbr": "AK", "latitude": 61.370716, "longitude": -152.404419},
    {"state": "Arizona", "state_abbr": "AZ", "latitude": 33.729759, "longitude": -111.431221},
    {"state": "Arkansas", "state_abbr": "AR", "latitude": 34.969704, "longitude": -92.373123},
    {"state": "California", "state_abbr": "CA", "latitude": 36.116203, "longitude": -119.681564},
    {"state": "Colorado", "state_abbr": "CO", "latitude": 39.059811, "longitude": -105.311104},
    {"state": "Connecticut", "state_abbr": "CT", "latitude": 41.597782, "longitude": -72.755371},
    {"state": "Delaware", "state_abbr": "DE", "latitude": 39.318523, "longitude": -75.507141},
    {"state": "Florida", "state_abbr": "FL", "latitude": 27.766279, "longitude": -81.686783},
    {"state": "Georgia", "state_abbr": "GA", "latitude": 33.040619, "longitude": -83.643074},
    {"state": "Hawaii", "state_abbr": "HI", "latitude": 21.094318, "longitude": -157.498337},
    {"state": "Idaho", "state_abbr": "ID", "latitude": 44.240459, "longitude": -114.478828},
    {"state": "Illinois", "state_abbr": "IL", "latitude": 40.349457, "longitude": -88.986137},
    {"state": "Indiana", "state_abbr": "IN", "latitude": 39.849426, "longitude": -86.258278},
    {"state": "Iowa", "state_abbr": "IA", "latitude": 42.011539, "longitude": -93.210526},
    {"state": "Kansas", "state_abbr": "KS", "latitude": 38.526600, "longitude": -96.726486},
    {"state": "Kentucky", "state_abbr": "KY", "latitude": 37.668140, "longitude": -84.670067},
    {"state": "Louisiana", "state_abbr": "LA", "latitude": 31.169546, "longitude": -91.867805},
    {"state": "Maine", "state_abbr": "ME", "latitude": 44.693947, "longitude": -69.381927},
    {"state": "Maryland", "state_abbr": "MD", "latitude": 39.063946, "longitude": -76.802101},
    {"state": "Massachusetts", "state_abbr": "MA", "latitude": 42.230171, "longitude": -71.530106},
    {"state": "Michigan", "state_abbr": "MI", "latitude": 43.326618, "longitude": -84.536095},
    {"state": "Minnesota", "state_abbr": "MN", "latitude": 45.694454, "longitude": -93.900192},
    {"state": "Mississippi", "state_abbr": "MS", "latitude": 32.741646, "longitude": -89.678696},
    {"state": "Missouri", "state_abbr": "MO", "latitude": 38.456085, "longitude": -92.288368},
    {"state": "Montana", "state_abbr": "MT", "latitude": 46.921925, "longitude": -110.454353},
    {"state": "Nebraska", "state_abbr": "NE", "latitude": 41.125370, "longitude": -98.268082},
    {"state": "Nevada", "state_abbr": "NV", "latitude": 38.313515, "longitude": -117.055374},
    {"state": "New Hampshire", "state_abbr": "NH", "latitude": 43.452492, "longitude": -71.563896},
    {"state": "New Jersey", "state_abbr": "NJ", "latitude": 40.298904, "longitude": -74.521011},
    {"state": "New Mexico", "state_abbr": "NM", "latitude": 34.840515, "longitude": -106.248482},
    {"state": "New York", "state_abbr": "NY", "latitude": 42.165726, "longitude": -74.948051},
    {"state": "North Carolina", "state_abbr": "NC", "latitude": 35.630066, "longitude": -79.806419},
    {"state": "North Dakota", "state_abbr": "ND", "latitude": 47.528912, "longitude": -99.784012},
    {"state": "Ohio", "state_abbr": "OH", "latitude": 40.388783, "longitude": -82.764915},
    {"state": "Oklahoma", "state_abbr": "OK", "latitude": 35.565342, "longitude": -96.928917},
    {"state": "Oregon", "state_abbr": "OR", "latitude": 44.572021, "longitude": -122.070938},
    {"state": "Pennsylvania", "state_abbr": "PA", "latitude": 40.590752, "longitude": -77.209755},
    {"state": "Rhode Island", "state_abbr": "RI", "latitude": 41.680893, "longitude": -71.511780},
    {"state": "South Carolina", "state_abbr": "SC", "latitude": 33.856892, "longitude": -80.945007},
    {"state": "South Dakota", "state_abbr": "SD", "latitude": 44.299782, "longitude": -99.438828},
    {"state": "Tennessee", "state_abbr": "TN", "latitude": 35.747845, "longitude": -86.692345},
    {"state": "Texas", "state_abbr": "TX", "latitude": 31.054487, "longitude": -97.563461},
    {"state": "Utah", "state_abbr": "UT", "latitude": 40.150032, "longitude": -111.862434},
    {"state": "Vermont", "state_abbr": "VT", "latitude": 44.045876, "longitude": -72.710686},
    {"state": "Virginia", "state_abbr": "VA", "latitude": 37.769337, "longitude": -78.169968},
    {"state": "Washington", "state_abbr": "WA", "latitude": 47.400902, "longitude": -121.490494},
    {"state": "West Virginia", "state_abbr": "WV", "latitude": 38.491226, "longitude": -80.954453},
    {"state": "Wisconsin", "state_abbr": "WI", "latitude": 44.268543, "longitude": -89.616508},
    {"state": "Wyoming", "state_abbr": "WY", "latitude": 42.755966, "longitude": -107.302490},
]


def get_states():
    """Return the state centroid table."""
    return pd.DataFrame(STATES)


def fetch_json(url, params):
    """Run one API request and return the JSON."""
    response = requests.get(url, params=params, timeout=120)
    response.raise_for_status()
    return response.json()


def build_daily_frame(state_row, payload):
    """Turn one Open-Meteo response into daily rows."""
    daily = payload["daily"]
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(daily["time"]),
            "temperature": pd.to_numeric(daily["temperature_2m_mean"], errors="coerce"),
            "precipitation": pd.to_numeric(daily["precipitation_sum"], errors="coerce"),
            "humidity": pd.to_numeric(daily["relative_humidity_2m_mean"], errors="coerce"),
        }
    )
    df["state"] = state_row["state"]
    df["state_abbr"] = state_row["state_abbr"]
    return df[["state", "state_abbr", "date", "temperature", "precipitation", "humidity"]]


def fetch_daily_weather(start_date, end_date):
    """Fetch daily weather for all 50 states."""
    states = get_states()
    rows = []

    for _, state_row in states.iterrows():
        logger.info("Fetching weather for %s", state_row["state_abbr"])
        params = {
            "latitude": state_row["latitude"],
            "longitude": state_row["longitude"],
            "timezone": "America/New_York",
            "daily": DAILY_VARS,
        }

        params["start_date"] = start_date
        params["end_date"] = end_date

        payload = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                payload = fetch_json(ARCHIVE_URL, params)
                break
            except requests.exceptions.ReadTimeout:
                print(
                    f"ReadTimeout for request: {state_row['state_abbr']} {start_date} to {end_date}. "
                    f"Retry {attempt}/{MAX_RETRIES}."
                )
                time.sleep(5)
            except requests.exceptions.HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 429:
                    wait_seconds = RATE_LIMIT_BACKOFF_SECONDS * attempt
                    print(
                        f"429 Too Many Requests for request: {state_row['state_abbr']} {start_date} to {end_date}. "
                        f"Retry {attempt}/{MAX_RETRIES} after {wait_seconds} seconds."
                    )
                    time.sleep(wait_seconds)
                    continue
                raise

        if payload is None:
            print(f"Skipping {state_row['state_abbr']} after {MAX_RETRIES} failed attempts.")
            continue

        rows.append(build_daily_frame(state_row, payload))
        time.sleep(REQUEST_DELAY_SECONDS)

    return pd.concat(rows, ignore_index=True)


def add_week_columns(df):
    """Add Wednesday to Tuesday week boundaries."""
    df = df.copy()
    offset = (df["date"].dt.weekday - 2) % 7
    df["week_start"] = df["date"] - pd.to_timedelta(offset, unit="D")
    df["week_end"] = df["week_start"] + pd.Timedelta(days=6)
    return df


def get_last_tuesday(day_value=None):
    """Return the most recent Tuesday on or before a date."""
    if day_value is None:
        day_value = date.today()
    days_back = (day_value.weekday() - 1) % 7
    return day_value - timedelta(days=days_back)


def daily_to_weekly(df):
    """Aggregate daily weather into weekly state rows."""
    if df.empty:
        return pd.DataFrame(
            columns=[
                "state_abbr",
                "week_start",
                "week_end",
                "week",
                "temp_avg_week",
                "precip_total_week",
                "humidity_avg_week",
            ]
        )

    df = add_week_columns(df)
    weekly = (
        df.groupby(["state_abbr", "week_start", "week_end"], as_index=False)
        .agg(
            temp_avg_week=("temperature", "mean"),
            precip_total_week=("precipitation", "sum"),
            humidity_avg_week=("humidity", "mean"),
        )
        .sort_values(["state_abbr", "week_start"])
    )

    week_info = weekly["week_end"].dt.isocalendar()
    weekly["week"] = week_info.week.astype(int)
    return weekly[
        [
            "state_abbr",
            "week_start",
            "week_end",
            "week",
            "temp_avg_week",
            "precip_total_week",
            "humidity_avg_week",
        ]
    ]


def load_weekly_data(csv_path):
    """Load an existing weekly dataset."""
    csv_file = Path(csv_path)
    if csv_file.exists():
        return pd.read_csv(csv_file, parse_dates=["week_start", "week_end"])
    return pd.DataFrame()


def combine_weekly_data(old_df, new_df):
    """Append new weeks and keep one row per state-week."""
    keep_columns = [
        "state_abbr",
        "week_start",
        "week_end",
        "week",
        "temp_avg_week",
        "precip_total_week",
        "humidity_avg_week",
    ]

    old_df = old_df.copy()
    new_df = new_df.copy()
    old_df = old_df.drop(columns=["state", "year", "pulled_at"], errors="ignore")
    new_df = new_df.drop(columns=["state", "year", "pulled_at"], errors="ignore")

    if old_df.empty:
        combined = new_df.copy()
    else:
        combined = pd.concat([old_df, new_df], ignore_index=True)

    combined["week_start"] = pd.to_datetime(combined["week_start"])
    combined["week_end"] = pd.to_datetime(combined["week_end"])
    combined = combined.drop_duplicates(subset=["state_abbr", "week_start"], keep="last")
    combined = combined.sort_values(["state_abbr", "week_start"]).reset_index(drop=True)
    return combined[keep_columns]


def save_data(df, csv_path):
    """Save a dataframe to CSV."""
    Path(csv_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False)
    logger.info("Saved %s", csv_path)
