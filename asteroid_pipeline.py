import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

import psycopg2
import requests
from database import AsteroidApproach, SessionLocal
from dotenv import load_dotenv
from sqlalchemy.exc import IntegrityError

load_dotenv(Path("../.env"))

nasa_key = os.getenv("NASA_KEY")


def retrieve_asteroids(start_date: str, end_date: str):
    """
    Fetching asteroid data from Nasa API
    Args:
        start_date: String in Format YYYY-MM-DD
        end_date : String in Format YYYY-MM-DD

    returns:
        dictionary with API response, or None if failed
    """
    # Build URL
    url = f"https://api.nasa.gov/neo/rest/v1/feed?start_date={start_date}&end_date={end_date}&api_key={nasa_key}"

    print(f"Fetching datas from {start_date} to {end_date}...")

    try:
        response = requests.get(url, timeout=30)

        if response.status_code == 200:
            print(f" ✓ Success! Got data.")
            data = response.json()
            return data
        else:
            print(f"  ✗ Error: Status code {response.status_code}")
            print(f"  Response: {response.text}")
            return None

    except Exception as e:
        print(f" x Request failed : {e}")
        return None


def parse_asteroid_data(data):
    """
    Parse nested Nasa API response into flat database records
    Args:
        Data: Json response from NASA API
    returns:
        List of dictionaries, each representing one asteroid approach
    """

    approaches = []

    for date, asteroids in data["near_earth_objects"].items():
        print(f" Parsing {len(asteroids)} asteroids for {date}")

        for asteroid in asteroids:
            # Extract Top-level Asteroid properties
            neo_id = asteroid["neo_reference_id"]
            name = asteroid["name"]
            nasa_jpl_url = asteroid.get("nasa_jpl_url")
            magnitude = asteroid.get("absolute_magnitude_h")

            # Extract Diameter(nested under 'estimated diameter')
            diameter = asteroid["estimated_diameter"]
            diam_km = diameter["kilometers"]
            diam_miles = diameter["miles"]

            # Boolean Flags:
            is_hazardous = asteroid["is_potentially_hazardous_asteroid"]
            is_sentry = asteroid.get("is_sentry_object", False)

            # Now process each close approach()
            for approach in asteroid["close_approach_data"]:
                record = {
                    "neo_reference_id": neo_id,
                    "name": name,
                    "nasa_jpl_url": nasa_jpl_url,
                    "absolute_magnitude_h": magnitude,
                    "estimated_diameter_km_min": diam_km["estimated_diameter_min"],
                    "estimated_diameter_km_max": diam_km["estimated_diameter_max"],
                    "estimated_diameter_miles_min": diam_miles[
                        "estimated_diameter_min"
                    ],
                    "estimated_diameter_miles_max": diam_miles[
                        "estimated_diameter_max"
                    ],
                    "is_potentially_hazardous": is_hazardous,
                    "is_sentry_object": is_sentry,
                    # Close approach data
                    "close_approach_date": datetime.strptime(
                        approach["close_approach_date"], "%Y-%m-%d"
                    ).date(),
                    "close_approach_date_full": approach.get(
                        "close_approach_date_full"
                    ),
                    "epoch_date_close_approach": approach.get(
                        "epoch_date_close_approach"
                    ),
                    # Velocity (3 different units)
                    "relative_velocity_kmh": float(
                        approach["relative_velocity"]["kilometers_per_hour"]
                    ),
                    "relative_velocity_kms": float(
                        approach["relative_velocity"]["kilometers_per_second"]
                    ),
                    "relative_velocity_miles_per_hour": float(
                        approach["relative_velocity"]["miles_per_hour"]
                    ),
                    # Distance:
                    "miss_distance_astronomical": float(
                        approach["miss_distance"]["astronomical"]
                    ),
                    "miss_distance_lunar": float(approach["miss_distance"]["lunar"]),
                    "miss_distance_km": float(approach["miss_distance"]["kilometers"]),
                    "miss_dstance_miles": float(approach["miss_distance"]["miles"]),
                    "orbiting_body": approach.get("orbiting_body", "Earth"),
                }
                approaches.append(record)
    return approaches


def load_asteroids_to_database(approach: list):
    """
    Load parsed asteroid approaches into the database
    Args:
        approaches: list of dictionaries (from parse_asteroid_data)
    """

    session = SessionLocal()
    loaded = 0
    skipped = 0

    print(f"\n Loading {len(approach)} records to database ...")

    try:
        for record in approach:
            try:
                # Create an AsteroidApproach object
                approach = AsteroidApproach(**record)

                # Add to session
                session.add(approach)

                # Commit to database
                session.commit()
                loaded += 1
            except IntegrityError:
                # Records already exists
                session.rollback()
                skipped += 1
            except Exception as e:
                print(f"  ✗ Error loading record: {e}")
                session.rollback()
    finally:
        # Close session
        session.close()
    print(f"  ✓ Loaded: {loaded}")
    print(f"  ⊘ Skipped (duplicates): {skipped}")

    return loaded, skipped


def backfill_asteroids(start_date_str: str, end_date_str, sleep_time=7):
    """
    Backfill asteroid data by fetching in 7 day chunks

    Args:
        start_date_str: Start date as YYYY-MM-DD
        end_date_str: End date as YYYY-MM-DD
        sleep_time: Seconds to wait between API calls (rate limiting)
    """

    # convert string to datetime objects:
    start = datetime.strptime(start_date_str, "%Y-%m-%d")
    end = datetime.strptime(end_date_str, "%Y-%m-%d")

    current_start = start
    batch_number = 0
    total_loaded = 0
    total_skipped = 0

    print(f"\n{'=' * 60}")
    print(f"Starting backfill from {start_date_str} to {end_date_str}")
    print(f"{'=' * 60}\n")

    while current_start < end:
        batch_number += 1

        # Calculating the end of this 7 day
        current_end = min(current_start + timedelta(days=6), end)
        print(
            f"\n [Batch {batch_number}] {current_start.date()} to {current_end.date()} "
        )
        data = retrieve_asteroids(
            current_start.strftime("%Y-%m-%d"), current_end.strftime("%Y-%m-%d")
        )

        if data:
            # Parse the data:
            approaches = parse_asteroid_data(data)
            # Load to database:
            loaded, skipped = load_asteroids_to_database(approaches)
            total_loaded += loaded
            total_skipped += skipped
        else:
            print("  ⚠️  Skipping this batch due to API error")

        current_start = current_end + timedelta(days=1)

        # Progress update every 10 batches
        if batch_number % 10 == 0:
            print(
                f"\n--- Progress: {batch_number} batches, {total_loaded:,} records loaded ---"
            )
        time.sleep(sleep_time)


if __name__ == "__main__":
    # Test 3 weeks:
    backfill_asteroids("2000-01-08", "2000-01-28")
