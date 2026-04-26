"""
Run basic QA checks on the processed flight master table.

This script helps decide whether the dataset is suitable for EDA and modelling.
It does not modify the dataset.
"""

from pathlib import Path

import pandas as pd


PROCESSED_INPUT_PATH = Path("data/processed/flights_master.csv")


def print_section(title: str) -> None:
    """Print a readable section heading."""
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def print_count_and_percent(label: str, count: int, total: int) -> None:
    """Print a count and percentage."""
    percent = (count / total * 100) if total > 0 else 0
    print(f"{label}: {count:,} / {total:,} ({percent:.2f}%)")


def load_master_table(path: Path = PROCESSED_INPUT_PATH) -> pd.DataFrame:
    """Load the processed master table."""
    if not path.exists():
        raise FileNotFoundError(
            f"Could not find {path}. Run preprocess.py first."
        )

    return pd.read_csv(path)


def run_qa_checks(df: pd.DataFrame) -> None:
    """Run basic QA checks and print results."""

    total_rows = len(df)

    print_section("1. Dataset size")
    print(f"Total rows: {total_rows:,}")

    if "flight_date" in df.columns:
        print(f"Date range: {df['flight_date'].min()} → {df['flight_date'].max()}")

    print("\nRows by airline group:")
    print(df["airline_group"].value_counts(dropna=False))

    print("\nRows by airline code:")
    print(df["airline_iata"].value_counts(dropna=False))

    print_section("2. Missingness of key columns")

    key_columns = [
        "scheduled_departure_time",
        "scheduled_arrival_time",
        "estimated_arrival_time",
        "actual_arrival_time",
        "api_arrival_minutes",
        "arrival_delay_minutes",
    ]

    for column in key_columns:
        if column in df.columns:
            missing = df[column].isna().sum()
            print_count_and_percent(f"Missing {column}", missing, total_rows)

    print_section("3. Arrival label quality")

    has_actual_arrival = df["actual_arrival_time"].notna()
    has_estimated_arrival = df["estimated_arrival_time"].notna()
    has_api_arrival_delay = df["api_arrival_delay_minutes"].notna()
    has_arrival_delay = df["arrival_delay_minutes"].notna()

    print_count_and_percent(
        "Rows with actual arrival time",
        has_actual_arrival.sum(),
        total_rows,
    )

    print_count_and_percent(
        "Rows with estimated arrival time",
        has_estimated_arrival.sum(),
        total_rows,
    )

    print_count_and_percent(
        "Rows with API-provided arrival delay",
        has_api_arrival_delay.sum(),
        total_rows,
    )

    print_count_and_percent(
        "Rows with arrival delay",
        has_arrival_delay.sum(),
        total_rows,
    )

    print_count_and_percent(
        "Estimated arrival exists but actual arrival missing",
        (has_estimated_arrival & ~has_actual_arrival).sum(),
        total_rows,
    )

    print_count_and_percent(
        "Actual arrival exists but arrival delay missing",
        (has_actual_arrival & ~has_arrival_delay).sum(),
        total_rows,
    )

    print_count_and_percent(
        "Arrival delay exists but actual arrival missing",
        (has_arrival_delay & ~has_actual_arrival).sum(),
        total_rows,
    )

    # api given vs derived actual arrival time
    if "arrival_delay_difference_api_vs_calculated" in df.columns:
        print("\nAPI delay vs calculated delay difference:")
        diff = pd.to_numeric(
            df["arrival_delay_difference_api_vs_calculated"],
            errors="coerce",
        )

        print(diff.describe())

        large_diff_count = diff.abs().gt(5).sum()
        print_count_and_percent(
            "Rows where API delay differs from calculated delay by >5 mins",
            large_diff_count,
            diff.notna().sum(),
        )

    print_section("4. Duplicate flight IDs")

    duplicate_count = df["flight_id"].duplicated().sum()
    print_count_and_percent("Duplicate flight_id rows", duplicate_count, total_rows)

    if duplicate_count > 0:
        print("\nExample duplicate flight IDs:")
        print(
            df.loc[df["flight_id"].duplicated(keep=False), "flight_id"]
            .value_counts()
            .head(10)
        )

    print_section("5. Flight status distribution")

    print(df["flight_status"].value_counts(dropna=False))

    print_section("6. Delay sanity checks")

    arrival_delay = pd.to_numeric(df["arrival_delay_minutes"], errors="coerce")

    print("Arrival delay summary:")
    print(arrival_delay.describe())

    print_count_and_percent(
        "Negative arrival delays",
        (arrival_delay < 0).sum(),
        total_rows,
    )

    print_count_and_percent(
        "Arrival delays over 12 hours",
        (arrival_delay > 720).sum(),
        total_rows,
    )

    print_section("7. Target balance")

    label_columns = [
        "arrival_delay_15_plus",
        "arrival_delay_60_plus",
        "arrival_delay_180_plus",
    ]

    for column in label_columns:
        if column in df.columns:
            valid = df[column].notna()
            positives = df[column].astype("boolean").sum(skipna=True)
            valid_count = valid.sum()

            percent = (positives / valid_count * 100) if valid_count > 0 else 0

            print(
                f"{column}: {positives:,} positives / "
                f"{valid_count:,} valid labels ({percent:.2f}%)"
            )


if __name__ == "__main__":
    master_df = load_master_table()
    run_qa_checks(master_df)