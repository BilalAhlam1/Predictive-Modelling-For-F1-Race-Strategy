import asyncio
import os
import aiohttp
import pandas as pd
from datetime import datetime, timedelta
import random
import math
import openf1_helper as of1
import sys, os; sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'DatabaseConnection')))
import databaseManager as db

SESSION_KEY = 9939
# Create output folder if it doesn't exist
output_folder = "RaceDataCSV"
os.makedirs(output_folder, exist_ok=True)
OUTPUT_CSV = os.path.join(output_folder, "session_{SESSION_KEY}_lap_locations.csv")

api = of1.api

# ---------------------------
# Async HTTP request helper with retries
# ---------------------------
async def fetch(session, url, params, max_retries=3):
    """Fetch data with retries on 429 errors."""
    for attempt in range(max_retries):
        try:
            # Make the request with aiohttp session to return JSON data asynchronously
            async with session.get(url, params=params) as response:
                if response.status == 429:
                    wait = (2 ** attempt) + random.uniform(1,2) # Exponential backoff with jitter
                    print(f"429 Too Many Requests. Retrying in {wait:.1f}s")
                    await asyncio.sleep(wait)
                    continue
                response.raise_for_status()
                data = await response.json()
                return data
        except aiohttp.ClientError as e:
            wait = (2 ** attempt) + random.uniform(1,2) # Exponential backoff with jitter
            print(f"HTTP error {e}. Retry in {wait:.1f}s")
            await asyncio.sleep(wait)
    print(f"Failed after {max_retries} retries for {params}")
    return []

# ---------------------------
# Fetch drivers for the session
# ---------------------------
async def get_drivers():
    """Get list of drivers for the session as (acronym, number) tuples."""
    df = api.get_dataframe('drivers', {'session_key': SESSION_KEY})
    if df.empty:
        print("No drivers found for this session.")
        return []
    return [(row['name_acronym'], row['driver_number']) for _, row in df.iterrows()]

# ---------------------------
# Fetch laps for a driver
# ---------------------------
async def get_laps(driver_number):
    """Get laps for a specific driver."""
    laps_df = api.get_dataframe('laps', {'session_key': SESSION_KEY, 'driver_number': driver_number})
    if laps_df.empty:
        return []
    laps_df = laps_df.sort_values('date_start') # Ensure laps are in chronological order
    return laps_df.to_dict('records')

# ---------------------------
# Fetch locations helper for a time range
# ---------------------------
async def get_locations(session, driver_number, start_iso, end_iso):
    """Fetch location data for a driver within a time range."""
    params = {
        'session_key': SESSION_KEY,
        'driver_number': driver_number,
        'date>': start_iso,
        'date<': end_iso
    }
    url = "https://api.openf1.org/v1/location"
    return await fetch(session, url, params)

# ---------------------------
# Process a single driver with batch requests
# ---------------------------
async def process_driver(driver_tuple, semaphore):
    """Process a single driver to fetch lap locations."""
    acronym, driver_number = driver_tuple
    print(f"Processing driver {acronym} ({driver_number})")
    records = []

    laps = await get_laps(driver_number)
    if not laps:
        return records

    # Filter out laps without start time to avoid issues to avoid DNF laps
    laps = [lap for lap in laps if lap.get('date_start')]
    if not laps:
        return records
    
    # Fetch locations in a single batch for all laps
    async with aiohttp.ClientSession() as session:
        # Determine full batch range for this driver
        last_lap_duration = laps[-1].get('lap_duration') # -1 to get last lap
        # Handle invalid lap durations
        if last_lap_duration is None or (isinstance(last_lap_duration, float) and math.isnan(last_lap_duration)):
            last_lap_duration = 0.0

        start_time = pd.to_datetime(laps[0]['date_start']) # first lap start
        end_time = pd.to_datetime(laps[-1]['date_start']) + timedelta(seconds=last_lap_duration) # last lap end

        start_iso = start_time.isoformat()
        end_iso = end_time.isoformat()

        # Fetch all locations for this batch
        async with semaphore: # semaphore is used to limit concurrent requests to avoid 429s (API rate limiting)
            locs = await get_locations(session, driver_number, start_iso, end_iso)

        # Assign locations back to individual laps by iterating through laps, filtering locs by lap time range and appending data
        if locs:
            locs_df = pd.DataFrame(locs)
            locs_df['date'] = pd.to_datetime(locs_df['date']) # Convert to datetime for filtering

            for lap in laps:
                lap_start = pd.to_datetime(lap['date_start'])
                lap_duration = lap.get('lap_duration')
                if lap_duration is None or (isinstance(lap_duration, float) and math.isnan(lap_duration)): # Handle invalid lap durations
                    lap_duration = 0.0
                lap_end = lap_start + timedelta(seconds=lap_duration) # Calculate lap end time

                lap_locs = locs_df[(locs_df['date'] >= lap_start) & (locs_df['date'] < lap_end)] # Filter locations for this lap
                for _, loc in lap_locs.iterrows():
                    records.append({
                        'session_key': SESSION_KEY,
                        'driver_acronym': acronym,
                        'driver_number': driver_number,
                        'lap_number': lap['lap_number'],
                        'lap_duration': lap_duration,
                        'timestamp': loc['date'].isoformat(),
                        'x': loc['x'],
                        'y': loc['y'],
                        'z': loc['z']
                    })

        await asyncio.sleep(0.1)  # small delay to reduce 429 risk

    print(f"Finished driver {acronym} ({len(records)} locations)")
    return records

# ---------------------------
# async runner
# ---------------------------
async def fetchWithAPI():
    drivers = await get_drivers() # List of (acronym, number)
    all_records = []

    #semaphore is a library to limit concurrent requests
    semaphore = asyncio.Semaphore(2)  # max 2 concurrent requests to reduce 429s

    # Create tasks for each driver and gather results asynchronously
    tasks = [process_driver(d, semaphore) for d in drivers]
    for future in asyncio.as_completed(tasks):
        result = await future
        all_records.extend(result)

    if not all_records:
        print("No location data collected.")
        return

    df = pd.DataFrame(all_records)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp')

    #df.to_csv(OUTPUT_CSV, index=False)
    #print(f"Saved all lap locations to {OUTPUT_CSV} ({len(df)} rows)")

    # Save to database
    #db.save_to_db(df, 'race_telemetry', if_exists='append')
    #print(f"Saved all lap locations to database table 'race_telemetry'.")
    return df

def fetchWithDB():
    "# Check if session data already exists in DB, otherwise run data collection"
    Sessions = db.load_from_db(f"""SELECT * FROM race_telemetry WHERE session_key = {SESSION_KEY}""")
    if Sessions.empty:
        print(f"Session {SESSION_KEY} not found in database.")
        df = asyncio.run(fetchWithAPI())
        db.save_to_db(df, 'race_telemetry', if_exists='append')
    else:
        print(f"Session {SESSION_KEY} found in database.")
        df = db.load_from_db(f"""SELECT * FROM race_telemetry WHERE session_key = {SESSION_KEY}""")

    return df

# ---------------------------
# Run
# ---------------------------
if __name__ == "__main__":
    "# Check if session data already exists in DB, otherwise run data collection through API"
    if db.test_db_connection():
        print("Database connection successful.")
        df = fetchWithDB()
        print(f"Data ready with {len(df)} rows for session {SESSION_KEY}.")
    else: 
        print("Database connection failed. Falling back to API fetch.")
        df = asyncio.run(fetchWithAPI())
        print(f"Data ready with {len(df)} rows for session {SESSION_KEY}.")

        