"""
Download raw historical flight schedule data from the Aviation Edge API.

This script is responsible only for:
- calling the Historical Schedules API
- saving raw JSON responses to data/raw/historical_schedules/

It does not clean, flatten, filter, or model the data.
"""

import json # to save results as .json
import os # reading env varaibles
from datetime import datetime, timedelta # For all work wth data ranges and windows
from pathlib import Path # simpelr path management

import time # for sleep between API calls
import requests # API Calls
from dotenv import load_dotenv # Loading .env file

# -----------------------------------------------
# Configuration
# -----------------------------------------------
load_dotenv()
API_KEY = os.getenv("AVIATION_EDGE_API_KEY")

if not API_KEY:
    raise ValueError("Missing API Key. Ensure .env file contains AVIATION_EDGE_API_KEY")

URL_ENDPOINT = "https://aviation-edge.com/v2/public/flightsHistory"
RAW_OUTPUT_DIR = Path("data/raw/historical_schedules")
DEFAULT_SCHEDULE_TYPE = "arrival" # whether to receive 'departure' or 'arrival' schedules
MAX_WINDOW = 30 # In days - max allowed search range as per Aviation Edge API restriction
REQUEST_TIMEOUT = 80 # In seconds
SLEEP_BETWEEN_CALLS = 0.2 # In seconds - to ensure API isn't overloaded
TARGET_AIRLINES = ["FR", "U2", "W6", "VY"] # Airline IATA Codes

# -----------------------------------------------
# Helper Functions
# -----------------------------------------------
def create_date_windows(start_date: str, end_date: str, window_days: int = MAX_WINDOW,
) -> list[tuple[str, str]]:
    """
    Split a date range into smaller date windows.

    The Aviation Edge Historical Schedules API allows up to 30 days
    per request, so this function turns a long date range into chunks.

    Args:
        start_date:
            First date to collect, in YYYY-MM-DD format.
        end_date:
            Last date to collect, in YYYY-MM-DD format.
        window_days:
            Maximum number of days per window.
    Returns:
        A list of (date_from, date_to) tuples as strings.
    """

    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()

    if start > end:
        raise ValueError("start_date must be before or equal to end_date")

    windows = []
    current_start = start

    while current_start <= end:
        current_end = current_start + timedelta(days=window_days -1)

        if current_end > end:
            current_end = end

        windows.append(
            (current_start.strftime("%Y-%m-%d"), current_end.strftime("%Y-%m-%d"))
        )

        current_start = current_end + timedelta(days=1)

    return windows

def build_output_path(
    airport_code: str, # IATA Code
    schedule_type: str, # Departure or arrival
    date_from: str, # Start date of API request
    date_to: str, # End date of API request
) -> Path:
    """
    Build a predicatable output path for raw API response

    Returns a Path object pointing to where the raw JSON should be saved
    """
    airport_code = airport_code.upper()
    schedule_type = schedule_type.lower()

    filename = f"{airport_code}_{schedule_type}_{date_from}_to_{date_to}.json"

    return RAW_OUTPUT_DIR / filename

# # Temp Tester function - build_output_path(), create_date_windows()
# if __name__ == "__main__":
#     test_windows = create_date_windows("2025-01-01", "2025-02-05")

#     print("Date windows:")
#     for date_from, date_to in test_windows:
#         print(date_from, "->", date_to)

#     test_path = build_output_path(
#         airport_code="stn",
#         schedule_type="departure",
#         date_from="2025-01-01",
#         date_to="2025-01-30",
#     )

#     print("\nExample output path:")
#     print(test_path)

# -----------------------------------------------
# Primary Functions
# -----------------------------------------------
def fetch_historical_schedules(
    airport_code: str,
    schedule_type: str,
    date_from: str,
    date_to: str,
) -> list | dict:
    """
    Fetch historical flight schedules from Aviation Edge API.

    Returns:
        Parsed JSON response (usually a list of flights).
    """

    # Filters allowed by Aviation Edge for the API call
    params = {
        "key": API_KEY,
        "code": airport_code,
        "type": schedule_type,
        "date_from": date_from,
        "date_to": date_to,
    }

    print("\n--- API CALL ---")
    print(f"Airport: {airport_code}")
    print(f"Type: {schedule_type}")
    print(f"Date range: {date_from} → {date_to}")

    # Build and send API request
    response = requests.get(
        URL_ENDPOINT,
        params=params,
        timeout=REQUEST_TIMEOUT,
    )

    print(f"Status code: {response.status_code}")
    response.raise_for_status() # Raise error if requests fails

    data = response.json() # Convert response to python object list[dict], each dict is one flight

    # Basic sanity check
    if isinstance(data, list):
        print(f"Flights returned: {len(data)}")
    else:
        print("Warning: response is not a list")
        print(data)

    return data

# Tester function for fetch_historical_schedules()
# if __name__ == "__main__":
#     data = fetch_historical_schedules(
#         airport_code="STN",
#         schedule_type="departure",
#         date_from="2026-01-21",
#         date_to="2026-01-21",
#     )

#     print("\nFirst record:")
#     if isinstance(data, list) and len(data) > 0:
#         print(json.dumps(data[0], indent=2))

def save_json(data: list | dict, output_path: Path) -> None:
    """
    Save API response data to a JSON file.

    output_path: File path where the JSON should be saved.
    """
    # Make sure output folder exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=2)

    print(f"Saved raw JSON to: {output_path}")

# # Tester Function
# if __name__ == "__main__":
#     airport_code = "STN"
#     schedule_type = "arrival"
#     date_from = "2026-01-21"
#     date_to = "2026-01-21"

#     data = fetch_historical_schedules(
#         airport_code=airport_code,
#         schedule_type=schedule_type,
#         date_from=date_from,
#         date_to=date_to,
#     )

#     output_path = build_output_path(
#         airport_code=airport_code,
#         schedule_type=schedule_type,
#         date_from=date_from,
#         date_to=date_to,
#     )

#     save_json(data, output_path)

#     print("\nFirst record:")
#     if isinstance(data, list) and len(data) > 0:
#         print(json.dumps(data[0], indent=2))

# -----------------------------------------------
# Data Pulls
# -----------------------------------------------
def run_single_test(
    airport_code: str = "STN",
    schedule_type: str = "arrival",
    date_from: str = "2026-01-21",
    date_to: str = "2026-01-21",
) -> None:
    """
    Run one small API pull and save the raw response.

    This is for testing the API with one airport and one date range
    before running a larger batch pull.
    """

    data = fetch_historical_schedules(
        airport_code=airport_code,
        schedule_type=schedule_type,
        date_from=date_from,
        date_to=date_to,
    )

    output_path = build_output_path(
        airport_code=airport_code,
        schedule_type=schedule_type,
        date_from=date_from,
        date_to=date_to,
    )

    save_json(data, output_path)

    if isinstance(data, list):
        print(f"\nSingle test complete. Rows saved: {len(data)}")
    else:
        print("\nSingle test complete, but response was not a list.")

def run_batch_pull(
    airport_codes: list[str],
    start_date: str,
    end_date: str,
    schedule_type: str = DEFAULT_SCHEDULE_TYPE,
    window_days: int = MAX_WINDOW,
    skip_existing: bool = True,
) -> None:
    """
    Run a batch pull over many airports and date windows.

    This function is designed to work for both:
    - small tests, e.g. 1 airport over 30 days
    - scaled pulls, e.g. many airports over a full year

    Args:
        airport_codes: List of airport IATA codes to query.
        start_date: First date to collect, in YYYY-MM-DD format.
        end_date: Last date to collect, in YYYY-MM-DD format.
        schedule_type: "arrival" or "departure".
        window_days: Number of days per API call. Aviation Edge allows up to 30.

        skip_existing:
            If True, skip API calls where the output file already exists.
            This makes the script safe to rerun after interruption.
    """
    start_time = time.perf_counter() # Start runtime timer

    # Break desired date range into API acceptable 30 day chunks
    date_windows = create_date_windows(
        start_date=start_date,
        end_date=end_date,
        window_days=window_days,
    )

    total_calls_planned = len(airport_codes) * len(date_windows)
    calls_completed = 0
    calls_skipped = 0
    calls_failed = 0
    total_rows_saved = 0

    print("\n=== Batch pull ready to start ===")
    print(f"Airports: {len(airport_codes)}")
    print(f"Date windows: {len(date_windows)}")
    print(f"Schedule type: {schedule_type}")
    print(f"Planned calls: {total_calls_planned}")

    # Confirm with user before starting
    print("Operation can be stopped now or during from terminal with CTRC + C")
    confirm = input("\nProceed with batch pull? (y/n): ").strip().lower()

    if confirm != "y":
        print("Opeartion not started. Cancelled by user.")
        return
    
    try:
        for airport_code in airport_codes:
            for date_from, date_to in date_windows:
                output_path = build_output_path(
                    airport_code=airport_code,
                    schedule_type=schedule_type,
                    date_from=date_from,
                    date_to=date_to,
                )

                if skip_existing and output_path.exists():
                    print(f"\nSkipping existing file: {output_path}")
                    calls_skipped += 1
                    continue

                try:
                    data = fetch_historical_schedules(
                        airport_code=airport_code,
                        schedule_type=schedule_type,
                        date_from=date_from,
                        date_to=date_to,
                    )

                    save_json(data, output_path)

                    if isinstance(data, list):
                        rows_saved = len(data)
                    else:
                        rows_saved = 0

                    total_rows_saved += rows_saved
                    calls_completed += 1

                    print(
                        f"Completed {calls_completed + calls_skipped + calls_failed}/"
                        f"{total_calls_planned} "
                        f"({airport_code}, {date_from} → {date_to}, rows={rows_saved})"
                    )
                
                except requests.RequestException as error:
                    calls_failed += 1
                    print(
                        f"\nRequest failed for {airport_code} "
                        f"{date_from} → {date_to}: {error}"
                    )

                except Exception as error:
                    calls_failed += 1
                    print(
                        f"\nUnexpected error for {airport_code} "
                        f"{date_from} → {date_to}: {error}"
                    )

                time.sleep(SLEEP_BETWEEN_CALLS)

    except KeyboardInterrupt:
        print("\n\nBatch pull interrupted by user (CTRL + C).")
        print("Progress so far:")
        print(f"Completed calls: {calls_completed}")
        print(f"Skipped calls: {calls_skipped}")
        print(f"Failed calls: {calls_failed}")
        print(f"Total rows saved: {total_rows_saved}")

    end_time = time.perf_counter()
        
    print("\n=== Batch pull complete ===")
    print(f"Planned calls: {total_calls_planned}")
    print(f"Completed calls: {calls_completed}")
    print(f"Skipped calls: {calls_skipped}")
    print(f"Failed calls: {calls_failed}")
    print(f"Total rows saved: {total_rows_saved}")
    print(f"Execution time: {end_time - start_time:.4f} seconds")

# -----------------------------------------------
# Main
# -----------------------------------------------
if __name__ == "__main__":
    # Small test settings
    TEST_AIRPORTS = ["STN", "LGW", "BCN", "MAD", "BUD", "LIS", "NCE", "ATH", "ZRH", "FCO"]
    TEST_START_DATE = "2025-07-01"
    TEST_END_DATE = "2025-07-30"
    SCHEDULE_TYPE = "arrival"

    run_batch_pull(
        airport_codes=TEST_AIRPORTS,
        start_date=TEST_START_DATE,
        end_date=TEST_END_DATE,
        schedule_type= SCHEDULE_TYPE,
        window_days=MAX_WINDOW,
        skip_existing=True,
    )