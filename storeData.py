import csv
import time
import random
import pandas as pd
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import openf1_helper as of1

# Initialize API client
api = of1.api

# Session key for brazil 2025
SESSION_KEY = 9869
OUTPUT_CSV = f"session_{SESSION_KEY}_racepoints.csv" # Output CSV file name


# Fetch drivers
def fetch_drivers(session_key):
    """Fetch drivers for a given session"""
    drivers_df = pd.DataFrame(requests.get(
        f"https://api.openf1.org/v1/drivers?session_key={SESSION_KEY}"
    ).json())

    if drivers_df.empty:
        print("No drivers found for this session.")
        exit()

    driver_map = {row['name_acronym']: row['driver_number'] for _, row in drivers_df.iterrows()} # Map of driver acronyms to numbers
    driver_list = list(driver_map.items()) # List of (acronym, number) tuples so we can fetch data in parallel

    teams = drivers_df.groupby('team_name')['name_acronym'].apply(list).to_dict()
    print("Drivers by Team:")
    print("=" * 40)
    for team, drivers in teams.items():
        driver_info = [f"{d} ({driver_map[d]})" for d in drivers]
        print(f"{team}: {', '.join(driver_info)}")
    return driver_list


# Fetch laps for a driver
def fetch_laps(session_key, driver_number):
    """Fetch laps for a given driver in a session"""
    url = f"https://api.openf1.org/v1/laps?session_key={session_key}&driver_number={driver_number}"
    resp = requests.get(url)
    resp.raise_for_status()
    laps = [lap for lap in resp.json() if lap.get("date_start")]
    laps.sort(key=lambda lap: lap["date_start"])
    return laps

# Fetch location samples with retries 
def fetch_locations_for_lap(session_key, driver_number, start_iso, end_iso, max_retries=3):
    """Fetch location samples for a driver between start and end timestamps of a lap"""
    for attempt in range(max_retries):
        try:
            url = (
                f"https://api.openf1.org/v1/location?"
                f"session_key={session_key}&driver_number={driver_number}&"
                f"date>{start_iso}&date<{end_iso}"
            )
            resp = requests.get(url)
            resp.raise_for_status()
            locs = resp.json()
            for loc in locs:
                loc["date"] = datetime.fromisoformat(loc["date"])
            locs.sort(key=lambda loc: loc["date"])
            print(f"Fetched {len(locs)} locations for driver {driver_number} between {start_iso} - {end_iso}")
            return locs
        except requests.HTTPError as e:
            if resp.status_code == 429:
                wait = (2 ** attempt) + random.random()
                print(f"429 Too Many Requests for driver {driver_number}. Retry {attempt+1} after {wait:.1f}s")
                time.sleep(wait)
            else:
                print(f"HTTPError for driver {driver_number}: {e}")
                raise
        except Exception as e:
            print(f"Error fetching locations for driver {driver_number}: {e}")
            time.sleep(0.5)
    print(f"Failed to fetch locations after {max_retries} attempts for driver {driver_number}")
    return []

# Fetch driver timeline
def fetch_driver_timeline(driver_tuple):
    """Fetch the full timeline of location samples for a driver"""
    acronym, driver_number = driver_tuple
    print(f"⏳ Starting driver {acronym} ({driver_number})")
    driver_records = []
    # Fetch laps
    try:
        laps = fetch_laps(SESSION_KEY, driver_number)
        if not laps:
            print(f"No laps found for {acronym}")
            return driver_records
        
        # Process each lap
        for i, lap in enumerate(laps):
            lap_num = lap["lap_number"]
            start_time = datetime.fromisoformat(lap["date_start"])
            lap_duration = lap.get("lap_duration") or 0.0 # in seconds and determines 0 if missing

            # Determine end time
            if i + 1 < len(laps) and laps[i + 1].get("date_start"):
                end_time = datetime.fromisoformat(laps[i + 1]["date_start"]) # Use next lap's start time as end time
            else:
                end_time = start_time + timedelta(seconds=lap_duration) # Fallback if no next lap

            locs = fetch_locations_for_lap(SESSION_KEY, driver_number,
                                           start_time.isoformat(), end_time.isoformat()) # Fetch locations for the lap
            if not locs:
                print(f"Skipping lap {lap_num} for {acronym} (no location data)")
                continue

            # Store location samples for the lap
            for loc in locs:
                driver_records.append({
                    "session_key": SESSION_KEY,
                    "driver_acronym": acronym,
                    "driver_number": driver_number,
                    "lap_number": lap_num,
                    "timestamp": loc["date"].isoformat(),
                    "x": loc["x"],
                    "y": loc["y"],
                    "z": loc["z"],
                })
            time.sleep(0.15)  # API rate limiting

        print(f"Finished driver {acronym} ({len(driver_records)} samples)")

    except Exception as e:
        print(f"Driver {acronym} failed: {e}")

    return driver_records

# Save all driver data to CSV
def save_to_csv(max_threads=1):
    """Fetch all drivers' timelines and save to CSV"""
    driver_list = fetch_drivers(SESSION_KEY)
    all_records = []
    print(f"Fetching all drivers with max_threads={max_threads}")
    # Fetch data in parallel for all drivers efficiently and faster
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = [executor.submit(fetch_driver_timeline, d) for d in driver_list] # fetch timelines for all drivers
        # Collect results
        for future in as_completed(futures):
            result = future.result()
            all_records.extend(result)

    if not all_records:
        print("No data to save.")
        return

    # Sort by timestamp for chronological order
    all_records.sort(key=lambda r: r["timestamp"])
    # Save to CSV with headers
    with open(OUTPUT_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "session_key", "driver_acronym", "driver_number",
            "lap_number", "timestamp", "x", "y", "z"
        ])
        writer.writeheader()
        writer.writerows(all_records)

    print(f"Saved ordered race data: {OUTPUT_CSV} ({len(all_records)} samples)")

if __name__ == "__main__":
    save_to_csv(max_threads=2)  # start with 2 thread to avoid 429 errors
    # TODO: Adjust max_threads based on observed API rate limits
    # TODO: Cache driver data to avoid re-fetching on reruns