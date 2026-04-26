"""
Preprocess raw Aviation Edge historical schedule JSON files.

This script reads raw JSON files downloaded by data_pull.py, flattens the
nested flight records into a tabular format, filters to target low-cost
carrier groups, and saves a clean master table for EDA/modelling.
"""

import json
from pathlib import Path
from typing import Any # for typehint

import pandas as pd

# -----------------------------------------------
# Config
# -----------------------------------------------
RAW_INPUT_DIR = Path("data/raw/historical_schedules")
PROCESSED_OUTPUT_PATH = Path("data/processed/flights_master.csv")

# Target airline groups
AIRLINE_GROUP_MAP = {
    # Ryanair Group
    "FR": "Ryanair Group",
    "RK": "Ryanair Group",
    "RR": "Ryanair Group",
    "AL": "Ryanair Group",

    # easyJet Group
    "U2": "easyJet Group",
    "EC": "easyJet Group",
    "DS": "easyJet Group",

    # Wizz Air Group
    "W6": "Wizz Air Group",
    "W9": "Wizz Air Group",
    "5W": "Wizz Air Group",
    "W4": "Wizz Air Group",

    # Vueling
    "VY": "Vueling",
}

TARGET_AIRLINE_CODES = set(AIRLINE_GROUP_MAP.keys())

# -----------------------------------------------
# Helper Functions
# -----------------------------------------------
def safe_get(data: dict[str, Any], *keys: str) -> Any:
    """
    Safely get a nested value from a dictionary.

    Incase API responses have missing fields.

    Example:
        safe_get(flight, "arrival", "actualTime")

    is safer than:
        flight["arrival"]["actualTime"]

    because it returns None if "arrival" or "actualTime" is missing.
    Args:
        data:
            Dictionary to search inside.
        *keys:
            Sequence of nested keys to follow.
    Returns:
        The value if found, otherwise None.
    """

    current = data

    for key in keys:
        if not isinstance(current, dict):
            return None
        
        current = current.get(key)

        if current is None:
            return None

    return current

def normalise_code(value: Any) -> str | None:
    """
    Convert airport/airline/flight codes to uppercase strings.

    Returns None if the value is missing.
    """
    if value is None:
        return None

    return str(value).strip().upper()

# -----------------------------------------------
# Loading and Flattening Json
# -----------------------------------------------
def flatten_schedule_record(record: dict[str, Any]) -> dict[str, Any]:
    """
    Flatten one Aviation Edge historical schedule record into one table row.

    Returns: A flat dictionary representing one flight row.
    """

    # Get all values we wnat to collect and store them (or none if missing) in table
    schedule_type = safe_get(record, "type")
    flight_status = safe_get(record, "status")

    airline_iata = normalise_code(safe_get(record, "airline", "iataCode"))
    airline_name = safe_get(record, "airline", "name")
    airline_group = AIRLINE_GROUP_MAP.get(airline_iata)

    flight_number = safe_get(record, "flight", "number")
    flight_iata = normalise_code(safe_get(record, "flight", "iataNumber"))

    origin_iata = normalise_code(safe_get(record, "departure", "iataCode"))
    destination_iata = normalise_code(safe_get(record, "arrival", "iataCode"))

    scheduled_departure_time = safe_get(record, "departure", "scheduledTime")
    actual_departure_time = safe_get(record, "departure", "actualTime")
    departure_delay_minutes = safe_get(record, "departure", "delay")

    scheduled_arrival_time = safe_get(record, "arrival", "scheduledTime")
    estimated_arrival_time = safe_get(record, "arrival", "estimatedTime")
    actual_arrival_time = safe_get(record, "arrival", "actualTime")
    api_arrival_delay_minutes = safe_get(record, "arrival", "delay")

    # Infer the flight route field from origin and destination
    if origin_iata and destination_iata:
        route = f"{origin_iata}_{destination_iata}"
    else:
        route = None

    is_codeshare = safe_get(record, "codeshared") is not None

    # A unique flight_id is needed to identify each flight and avoid confusion
    flight_id_parts = [
        airline_iata,
        str(flight_number) if flight_number is not None else None,
        origin_iata,
        destination_iata,
        scheduled_departure_time,
    ]

    if all(flight_id_parts):
        flight_id = "_".join(flight_id_parts)
    else:
        flight_id = None

    return {
        "flight_id": flight_id,
        "schedule_type": schedule_type,
        "flight_status": flight_status,

        "airline_iata": airline_iata,
        "airline_name": airline_name,
        "airline_group": airline_group,

        "flight_number": flight_number,
        "flight_iata": flight_iata,

        "origin_iata": origin_iata,
        "destination_iata": destination_iata,
        "route": route,

        "scheduled_departure_time": scheduled_departure_time,
        "actual_departure_time": actual_departure_time,
        "departure_delay_minutes": departure_delay_minutes,

        "scheduled_arrival_time": scheduled_arrival_time,
        "estimated_arrival_time": estimated_arrival_time,
        "actual_arrival_time": actual_arrival_time,
        "api_arrival_delay_minutes": api_arrival_delay_minutes,

        "is_codeshare": is_codeshare,
    }

# # Test
# if __name__ == "__main__":
#     example_record = {
#         "type": "arrival",
#         "status": "landed",
#         "departure": {
#             "iataCode": "mad",
#             "delay": 68,
#             "scheduledTime": "2026-01-21t14:15:00.000",
#             "actualTime": "2026-01-21t15:22:00.000",
#         },
#         "arrival": {
#             "iataCode": "ath",
#             "delay": 60,
#             "scheduledTime": "2026-01-21t18:45:00.000",
#         },
#         "airline": {
#             "name": "sky express",
#             "iataCode": "gq",
#         },
#         "flight": {
#             "number": "921",
#             "iataNumber": "gq921",
#         },
#     }

#     flattened = flatten_schedule_record(example_record)

#     print(json.dumps(flattened, indent=2))

def load_json_file(file_path: Path) -> list[dict[str, Any]]:
    """
    Load one raw Aviation Edge JSON file.

    Returns:
        A list of raw flight records.
        If the file does not contain a list, returns an empty list.
    """

    with open(file_path, "r", encoding="utf-8") as file:
        data = json.load(file)

    if not isinstance(data, list):
        print(f"Warning: {file_path} did not contain a list. Skipping.")
        return []

    return data

def load_all_raw_records(raw_dir: Path = RAW_INPUT_DIR) -> list[dict[str, Any]]:
    """
    Load all raw historical schedule JSON files from a directory.

    Returns: A combined list of all raw flight records.
    """

    json_files = sorted(raw_dir.glob("*.json"))

    print(f"Raw JSON files found: {len(json_files)}")

    all_records = []

    for file_path in json_files:
        records = load_json_file(file_path)
        all_records.extend(records)

    print(f"Raw flight records loaded: {len(all_records)}")

    return all_records

# if __name__ == "__main__":
#     raw_records = load_all_raw_records()

#     print("\nFirst 3 flattened records:")
#     for record in raw_records[:3]:
#         flattened = flatten_schedule_record(record)
#         print(json.dumps(flattened, indent=2))
#         print("-" * 80)

# -----------------------------------------------
# Build and Save Table
# -----------------------------------------------
def build_master_table(raw_records: list[dict[str, Any]]) -> pd.DataFrame:
    """
    Build the master flights table from raw Aviation Edge records.

    raw_records: List of raw nested flight records.

    Returns: A pandas DataFrame with one row per flight.
    """

    flattened_rows = []
    for record in raw_records:
        flattened = flatten_schedule_record(record)
        flattened_rows.append(flattened)

    df = pd.DataFrame(flattened_rows) # List -> Table

    print(f"\nRows after flattening: {len(df)}")

    # Filter to the airline groups we care about.
    before_filter = len(df)
    df = df[df["airline_iata"].isin(TARGET_AIRLINE_CODES)].copy()
    after_filter = len(df)

    print(f"Rows after target airline filter: {after_filter}")
    print(f"Rows removed by airline filter: {before_filter - after_filter}")

    # Remove duplicate flight IDs.
    before_dedup = len(df)
    df = df.drop_duplicates(subset=["flight_id"])
    after_dedup = len(df)

    print(f"Duplicate flight IDs removed: {before_dedup - after_dedup}")

    # Convert delay fields to numeric.
    df["departure_delay_minutes"] = pd.to_numeric(
        df["departure_delay_minutes"],
        errors="coerce",
    )

    df["api_arrival_delay_minutes"] = pd.to_numeric(
        df["api_arrival_delay_minutes"],
        errors="coerce",
    )

    # Parse arrival timestamps.
    # Missing or invalid timestamp values become NaT.
    df["scheduled_arrival_time_parsed"] = pd.to_datetime(
        df["scheduled_arrival_time"],
        errors="coerce",
    )

    df["actual_arrival_time_parsed"] = pd.to_datetime(
        df["actual_arrival_time"],
        errors="coerce",
    )

    # Calculate arrival delay ourselves from scheduled and actual arrival times.
    # This becomes the main label source.
    df["arrival_delay_minutes"] = (
        df["actual_arrival_time_parsed"] - df["scheduled_arrival_time_parsed"]
    ).dt.total_seconds() / 60

    # Keep a QA comparison between the API-provided delay and our calculated delay.
    df["arrival_delay_difference_api_vs_calculated"] = (
        df["api_arrival_delay_minutes"] - df["arrival_delay_minutes"]
    )

    # Basic data availability flags.
    df["has_actual_arrival_time"] = df["actual_arrival_time"].notna()
    df["has_estimated_arrival_time"] = df["estimated_arrival_time"].notna()
    df["has_api_arrival_delay"] = df["api_arrival_delay_minutes"].notna()
    df["has_arrival_delay"] = df["arrival_delay_minutes"].notna()

    # Create labels.
    # Use pandas nullable boolean dtype so missing labels can be pd.NA.
    df["arrival_delay_15_plus"] = df["arrival_delay_minutes"].ge(15).astype("boolean")
    df["arrival_delay_60_plus"] = df["arrival_delay_minutes"].ge(60).astype("boolean")
    df["arrival_delay_180_plus"] = df["arrival_delay_minutes"].ge(180).astype("boolean")

    missing_arrival_delay = df["arrival_delay_minutes"].isna()

    df.loc[missing_arrival_delay, "arrival_delay_15_plus"] = pd.NA
    df.loc[missing_arrival_delay, "arrival_delay_60_plus"] = pd.NA
    df.loc[missing_arrival_delay, "arrival_delay_180_plus"] = pd.NA

    missing_arrival_delay = df["arrival_delay_minutes"].isna()

    df.loc[missing_arrival_delay, "arrival_delay_15_plus"] = pd.NA
    df.loc[missing_arrival_delay, "arrival_delay_60_plus"] = pd.NA
    df.loc[missing_arrival_delay, "arrival_delay_180_plus"] = pd.NA

    # Parse scheduled departure time for basic date/time fields.
    df["scheduled_departure_time_parsed"] = pd.to_datetime(
        df["scheduled_departure_time"],
        errors="coerce",
    )

    df["flight_date"] = df["scheduled_departure_time_parsed"].dt.date
    df["scheduled_departure_hour"] = df["scheduled_departure_time_parsed"].dt.hour
    df["scheduled_departure_day_of_week"] = df[
        "scheduled_departure_time_parsed"
    ].dt.day_name()
    df["scheduled_departure_month"] = df["scheduled_departure_time_parsed"].dt.month

    print(f"Rows with arrival delay: {df['has_arrival_delay'].sum()}")
    print(f"Rows with actual arrival time: {df['has_actual_arrival_time'].sum()}")

    return df

# Save master table to disk
def save_master_table(df: pd.DataFrame, output_path: Path = PROCESSED_OUTPUT_PATH) -> None:
  
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(output_path, index=False)

    print(f"\nSaved master table to: {output_path}")

if __name__ == "__main__":
    raw_records = load_all_raw_records()
    master_df = build_master_table(raw_records)
    save_master_table(master_df)

    print("\nMaster table preview:")
    print(master_df.head())

    print("\nColumns:")
    print(master_df.columns.tolist())