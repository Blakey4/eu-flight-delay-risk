"""
Small first test for the Aviation Edge Historical Schedules API.

This script:
1. Loads your API key from the local .env file.
2. Makes one small request to the flightsHistory endpoint.
3. Prints basic information about the response.
4. Saves the raw JSON response into data/raw/.

Run from the project root:

    python src/data_pull.py
"""

import json
import os
from pathlib import Path

import requests
from dotenv import load_dotenv
from datetime import datetime

# Load Environment Variables from the .env file
# This lets me use the API key without hardcoding it in the script.
load_dotenv()

API_KEY = os.getenv("AVIATION_EDGE_API_KEY")

if not API_KEY:
    raise ValueError("API key not found. Please set AVIATION_EDGE_API_KEY in your .env file.")

# ---------------------------------------------------------------------
# 2. API Endpoint
# ---------------------------------------------------------------------
BASE_URL = "https://aviation-edge.com/v2/public/flightsHistory"

# ---------------------------------------------------------------------
# 3. Parameters for the API Request
# ---------------------------------------------------------------------
parameters = {
    "key": API_KEY,
    "code": "CHQ", # Airport IATA code
    "type": "arrival", # "departure" or "arrival"
    "date_from": "2026-01-21", # Start date in YYYY-MM-DD format
    "airline_iata": "gq", # Optional: Filter by airline IATA code
    "flight_num": "921", # Optional: Filter by flight number
}

# ---------------------------------------------------------------------
# 4. Make the API request
# ---------------------------------------------------------------------

print("Making API request...")
print(f"Endpoint: {BASE_URL}")

# Do not print full params dictionary because it has API key
safe_params = {key: value for key, value in parameters.items() if key != "key"}
print(f"Params: {safe_params}")

response = requests.get(BASE_URL,params=parameters, timeout=10)

# ---------------------------------------------------------------------
# 5. Check whether the request succeeded
# ---------------------------------------------------------------------
print(f"HTTP status code: {response.status_code}")

# HTTP status codes:
# 200 = OK
# 400 = bad request
# 401/403 = auth/access problem
# 404 = endpoint not found
# 500 = server error
#
# raise_for_status() raises an error if the status code is bad.
response.raise_for_status()

# ---------------------------------------------------------------------
# 6. Parse JSON response
# ---------------------------------------------------------------------
data = response.json()

print(f"Python type of response data: {type(data)}")

if isinstance(data, list):
    print(f"Number of rows returned: {len(data)}")

    if len(data) > 0:
        print("\nFirst row:")
        print(json.dumps(data[0], indent=2))
    else:
        print("No rows returned for this query.")

elif isinstance(data, dict):
    print("Response is a dictionary, not a list.")
    print(json.dumps(data, indent=2))

else:
    print("Unexpected response type.")
    print(data)


# ---------------------------------------------------------------------
# 7. Save raw JSON response
# ---------------------------------------------------------------------
raw_dir = Path("data/raw")
raw_dir.mkdir(parents=True, exist_ok=True)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_path = raw_dir / f"historical_schedules_test_{timestamp}.json"

with open(output_path, "w", encoding="utf-8") as file:
    json.dump(data, file, indent=2)

print(f"\nSaved raw response to: {output_path}")