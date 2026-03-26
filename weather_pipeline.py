import logging
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests


logger = logging.getLogger(__name__)

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
ARCHIVE_DAILY_VARS = "temperature_2m_mean,precipitation_sum,relative_humidity_2m_mean"
FORECAST_DAILY_VARS = "temperature_2m_mean,precipitation_sum"
HOURLY_HUMIDITY_VAR = "relative_humidity_2m"
DEFAULT_DATA_PATH = "data/weather_state_weekly.csv"
WEEKLY_DATA_COLUMNS = [
    "state_abbr",
    "week_start",
    "week_end",
    "week",
    "temp_avg_week",
    "precip_total_week",
    "humidity_avg_week",
    "data_type",
]
UNIQUE_WEEK_COLUMNS = ["state_abbr", "week_start", "week_end"]
DATA_TYPE_PRIORITY = {"forecast": 0, "observed": 1}
MAX_RETRIES = 5
REQUEST_DELAY_SECONDS = 1
BASE_BACKOFF_SECONDS = 5


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
    """Run one API request with retry/backoff and return the JSON."""
    last_exception = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, timeout=120)
            response.raise_for_status()
            return response.json()
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as exc:
            last_exception = exc
            wait_seconds = BASE_BACKOFF_SECONDS * attempt
            logger.warning("Request failed (%s). Retry %s/%s in %ss.", exc.__class__.__name__, attempt, MAX_RETRIES, wait_seconds)
            time.sleep(wait_seconds)
        except requests.exceptions.HTTPError as exc:
            last_exception = exc
            status_code = exc.response.status_code if exc.response is not None else None
            if status_code == 429 or (status_code is not None and 500 <= status_code < 600):
                wait_seconds = BASE_BACKOFF_SECONDS * attempt
                logger.warning(
                    "HTTP %s from Open-Meteo. Retry %s/%s in %ss.",
                    status_code,
                    attempt,
                    MAX_RETRIES,
                    wait_seconds,
                )
                time.sleep(wait_seconds)
                continue
            raise

    raise RuntimeError(f"Open-Meteo request failed after {MAX_RETRIES} attempts: {last_exception}") from last_exception


def hourly_to_daily_humidity(payload):
    """Aggregate hourly humidity into daily mean humidity for fallback use."""
    hourly = payload.get("hourly") or {}
    hourly_times = hourly.get("time")
    humidity_values = hourly.get(HOURLY_HUMIDITY_VAR)
    if not hourly_times or humidity_values is None:
        return pd.DataFrame(columns=["date", "humidity"])

    hourly_df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(hourly_times),
            "humidity": pd.to_numeric(humidity_values, errors="coerce"),
        }
    )
    hourly_df["date"] = hourly_df["timestamp"].dt.floor("D")
    return hourly_df.groupby("date", as_index=False)["humidity"].mean()


def build_daily_frame(state_row, payload):
    """Turn one Open-Meteo response into daily rows, preserving humidity fallback."""
    daily = payload["daily"]
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(daily["time"]),
            "temperature": pd.to_numeric(daily["temperature_2m_mean"], errors="coerce"),
            "precipitation": pd.to_numeric(daily["precipitation_sum"], errors="coerce"),
        }
    )

    if "relative_humidity_2m_mean" in daily:
        df["humidity"] = pd.to_numeric(daily["relative_humidity_2m_mean"], errors="coerce")
    else:
        df["humidity"] = pd.NA

    # Open-Meteo sometimes exposes humidity only hourly. Fill daily gaps from hourly means.
    hourly_humidity = hourly_to_daily_humidity(payload)
    if not hourly_humidity.empty:
        df = df.merge(hourly_humidity, on="date", how="left", suffixes=("", "_hourly"))
        df["humidity"] = df["humidity"].fillna(df["humidity_hourly"])
        df = df.drop(columns=["humidity_hourly"])

    df["state"] = state_row["state"]
    df["state_abbr"] = state_row["state_abbr"]
    return df[["state", "state_abbr", "date", "temperature", "precipitation", "humidity"]]


def fetch_daily_weather(start_date, end_date, *, source):
    """Fetch daily weather for all 50 states from archive or forecast."""
    if source == "observed":
        url = ARCHIVE_URL
        daily_vars = ARCHIVE_DAILY_VARS
    elif source == "forecast":
        url = FORECAST_URL
        daily_vars = FORECAST_DAILY_VARS
    else:
        raise ValueError(f"Unsupported weather source: {source}")

    states = get_states()
    rows = []

    for _, state_row in states.iterrows():
        logger.info("Fetching %s weather for %s", source, state_row["state_abbr"])
        params = {
            "latitude": state_row["latitude"],
            "longitude": state_row["longitude"],
            "timezone": "America/New_York",
            "daily": daily_vars,
            "hourly": HOURLY_HUMIDITY_VAR,
            "start_date": start_date,
            "end_date": end_date,
        }

        payload = fetch_json(url, params)
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


def normalize_weekly_schema(df):
    """Bring legacy weekly data into the new schema without changing row grain."""
    if df is None or df.empty:
        return pd.DataFrame(columns=WEEKLY_DATA_COLUMNS)

    normalized = df.copy()
    normalized = normalized.drop(columns=["state", "year", "pulled_at"], errors="ignore")
    normalized["week_start"] = pd.to_datetime(normalized["week_start"])
    normalized["week_end"] = pd.to_datetime(normalized["week_end"])

    if "data_type" not in normalized.columns:
        normalized["data_type"] = "observed"
    normalized["data_type"] = normalized["data_type"].fillna("observed")
    normalized = normalized.drop(columns=["generated_at"], errors="ignore")

    return normalized[WEEKLY_DATA_COLUMNS]


def daily_to_weekly(df, *, data_type):
    """Aggregate daily weather into weekly state rows with row metadata."""
    if df.empty:
        return pd.DataFrame(columns=WEEKLY_DATA_COLUMNS)

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
    weekly["data_type"] = data_type
    return weekly[WEEKLY_DATA_COLUMNS]


def load_weekly_data(csv_path):
    """Load an existing weekly dataset."""
    csv_file = Path(csv_path)
    if csv_file.exists():
        return pd.read_csv(csv_file, parse_dates=["week_start", "week_end"])
    return pd.DataFrame(columns=WEEKLY_DATA_COLUMNS)


def combine_weekly_data(old_df, new_df):
    """Merge weekly rows, replacing same-week forecasts with observations."""
    old_df = normalize_weekly_schema(old_df)
    new_df = normalize_weekly_schema(new_df)

    combined = pd.concat([old_df, new_df], ignore_index=True)
    combined["data_type_priority"] = combined["data_type"].map(DATA_TYPE_PRIORITY).fillna(-1)

    # For each state-week, keep observed rows ahead of forecast rows. For duplicates with the
    # same data_type, keep the later row from the concat so reruns overwrite older values safely.
    combined = combined.sort_values(
        UNIQUE_WEEK_COLUMNS + ["data_type_priority"],
        ascending=[True, True, True, True],
        kind="mergesort",
    )
    combined = combined.drop_duplicates(subset=UNIQUE_WEEK_COLUMNS, keep="last")
    combined = combined.drop(columns=["data_type_priority"])
    combined = combined.sort_values(["state_abbr", "week_start"]).reset_index(drop=True)
    return combined[WEEKLY_DATA_COLUMNS]


def next_forecast_window(latest_observed_week_end):
    """Return the next Wednesday-to-Tuesday week immediately after the latest observed week."""
    week_start = pd.Timestamp(latest_observed_week_end) + pd.Timedelta(days=1)
    week_end = week_start + pd.Timedelta(days=6)
    return week_start.date(), week_end.date()


def enforce_single_forecast_week(df, forecast_week_start, forecast_week_end):
    """Keep only the target forecast week while preserving all observed history."""
    weekly = normalize_weekly_schema(df)
    forecast_week_start = pd.Timestamp(forecast_week_start)
    forecast_week_end = pd.Timestamp(forecast_week_end)

    observed = weekly.loc[weekly["data_type"] != "forecast"]
    forecast = weekly.loc[
        (weekly["data_type"] == "forecast")
        & (weekly["week_start"] == forecast_week_start)
        & (weekly["week_end"] == forecast_week_end)
    ]
    return (
        pd.concat([observed, forecast], ignore_index=True)
        .sort_values(["state_abbr", "week_start"])
        .reset_index(drop=True)[WEEKLY_DATA_COLUMNS]
    )


def save_data(df, csv_path):
    """Save weekly data to CSV and parquet side by side."""
    output = normalize_weekly_schema(df)
    csv_file = Path(csv_path)
    parquet_file = csv_file.with_suffix(".parquet")

    csv_file.parent.mkdir(parents=True, exist_ok=True)
    output.to_csv(csv_file, index=False)
    output.to_parquet(parquet_file, index=False)
    logger.info("Saved %s and %s", csv_file, parquet_file)
