import asyncio
import os
import time
import aiohttp
import pandas as pd
from datetime import datetime, timedelta
import random
import math
import openf1_helper as of1
import sys, os; sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'DatabaseConnection')))
import databaseManager as db
import matplotlib.pyplot as plt
api = of1.api

# ---------------------------
# Async HTTP request helper with retries increased to 5
# ---------------------------
async def fetch(session, url, params, max_retries=5):
    """Fetch data with retries on 429 errors."""
    for attempt in range(max_retries):
        try:
            timeout = aiohttp.ClientTimeout(total=60)  # 60 seconds timeout
            # Make the request with aiohttp session to return JSON data asynchronously
            async with session.get(url, params=params) as response:
                if response.status == 429:
                    wait = (2 ** attempt) + random.uniform(2, 4) # Exponential backoff with jitter increased to 2-4 seconds
                    print(f"429 Too Many Requests. Retrying in {wait:.1f}s")
                    await asyncio.sleep(wait)
                    continue
                if response.status >= 500:
                    # Server error, wait and retry with increased delay
                    wait = 5
                    print(f"Server error {response.status}. Retrying in {wait}s...")
                    await asyncio.sleep(wait)
                    continue
                    
                response.raise_for_status()
                data = await response.json()
                return data
        except aiohttp.ClientError as e:
            wait = (2 ** attempt) + random.uniform(1,2)
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
    
    # Determine full time range to fetch in chunks
    last_lap_duration = laps[-1].get('lap_duration')
    if last_lap_duration is None or (isinstance(last_lap_duration, float) and math.isnan(last_lap_duration)):
        last_lap_duration = 0.0

    start_time = pd.to_datetime(laps[0]['date_start'])
    end_time = pd.to_datetime(laps[-1]['date_start']) + timedelta(seconds=last_lap_duration)

    # chunking logic
    # Split the total time into 30-minute chunks to avoid overloading the API
    chunk_size = timedelta(minutes=30)
    current_start = start_time
    all_locs = []

    async with aiohttp.ClientSession() as session:
        async with semaphore:
            while current_start < end_time:
                current_end = min(current_start + chunk_size, end_time)
                
                # Fetch chunk
                chunk_data = await get_locations(
                    session, 
                    driver_number, 
                    current_start.isoformat(), 
                    current_end.isoformat()
                )
                all_locs.extend(chunk_data)
                
                # Move to next chunk
                current_start = current_end
                
                # Small polite delay between chunks for the same driver to prevent rate limiting
                await asyncio.sleep(0.5) 

    # If no locations found after all chunks, return empty
    if not all_locs:
        print(f"Warning: No locations found for {acronym}")
        return records

    # Assign locations back to individual laps
    locs_df = pd.DataFrame(all_locs)
    
    # Drop duplicates that might occur at chunk boundaries
    locs_df = locs_df.drop_duplicates(subset=['date'])
    
    locs_df['date'] = pd.to_datetime(locs_df['date'], format = 'ISO8601', errors='coerce')

    # Optimized: Vectorized lookup or standard iteration (Standard is fine for this volume)
    for lap in laps:
        lap_start = pd.to_datetime(lap['date_start'])
        lap_duration = lap.get('lap_duration')
        if lap_duration is None or (isinstance(lap_duration, float) and math.isnan(lap_duration)):
            lap_duration = 0.0
        lap_end = lap_start + timedelta(seconds=lap_duration)

        # Filter locations for this lap
        mask = (locs_df['date'] >= lap_start) & (locs_df['date'] < lap_end)
        lap_locs = locs_df[mask]
        
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
    df['timestamp'] = pd.to_datetime(df['timestamp'], format = 'ISO8601', errors='coerce')
    df = df.sort_values('timestamp')
    
    return df

# ---------------------------
# fetch from DB
# ---------------------------
def fetchFromDB(session_key):
    """Fetch session data directly from the database."""
    df = db.load_from_db(f"""SELECT * FROM race_telemetry WHERE session_key = {session_key}""")
    return df

# ---------------------------
# update and store to DB
# ---------------------------
def updateDB():
    """
    Checks DB for data, fetches from API if missing.
    Returns True if successful, False if failed.
    """
    try:
        # Check if session data already exists in DB
        Sessions = db.load_from_db(f"SELECT * FROM race_telemetry WHERE session_key = {SESSION_KEY}")
        
        if Sessions.empty:
            print(f"Session {SESSION_KEY} not found in database. Fetching...")
            try:
                df = asyncio.run(fetchWithAPI())
            except Exception as e:
                print(f"Error running async fetch: {e}")
                return False

            # Check if API actually returned data
            if df is None or df.empty:
                print(f"API returned no data for session {SESSION_KEY}.")
                return False

            try:
                db.save_to_db(df, 'race_telemetry', if_exists='append')
                print(f"Successfully saved session {SESSION_KEY} to database.")
                return True
            except Exception as e:
                print(f"Error saving to database: {e}")
                return False
        else:
            print(f"Session {SESSION_KEY} found in database.")
            return True # Data exists, so this is a success

    except Exception as e:
        print(f"Unexpected error in updateDB for session {SESSION_KEY}: {e}")
        return False


# ---------------------------
# check connection and store
# ---------------------------
def check_and_update_DB(session_key):
    """
    Sets global session key and attempts update.
    Returns True if successful, False otherwise.
    """
    global SESSION_KEY
    SESSION_KEY = session_key
    
    if db.test_db_connection():
        print("Database connection successful.")
        # Return the result of the update operation
        if updateDB():
            print(f"Data ready for session {SESSION_KEY}.")
            return True
        else:
            print(f"Failed to update/verify data for session {SESSION_KEY}.")
            return False
    else: 
        print("Database connection failed. Falling back to API fetch.")
        # If DB is down, we return False as we can't "update" the DB
        return False

# ---------------------------
# Update last five sessions and store if not present
# ---------------------------
def update_last_five_sessions():
    """
    Fetch the last five session keys and update DB.
    Returns True only if ALL 5 sessions are successfully processed/verified.
    """
    try:
        sessions_df = api.get_dataframe('sessions', {
            'year': time.localtime().tm_year,
            'session_type': 'Race'
        })
    except Exception as e:
        print(f"Error fetching session list: {e}")
        return False

    if sessions_df.empty:
        print("No sessions found.")
        return False

    sessions_df = sessions_df.sort_values('date_start')
    recent_sessions = sessions_df.tail(5)
    
    all_success = True

    for _, session in recent_sessions.iterrows():
        # returns True/False based on success
        result = check_and_update_DB(session['session_key'])
        if not result:
            all_success = False
            print(f"Issue processing session {session['session_key']}")

    #remove all sessions not in recent five from the database
    # UPDATE: Handle edge cases for NOT IN clause with fewer than 5 sessions
    recent_keys = sessions_df['session_key'].tail(5).tolist()
    try:
        if len(recent_keys) == 0:
            # Danger: If list is empty, NOT IN () is invalid SQL
            print("No recent keys provided. Skipping delete to prevent error.")
        elif len(recent_keys) == 1:
            # Handle single item (remove trailing comma)
            keys_str = f"({recent_keys[0]})"
            query = f"DELETE FROM race_telemetry WHERE session_key NOT IN {keys_str}"
            db.execute_query(query)
        else:
            # Handle multiple items
            query = f"DELETE FROM race_telemetry WHERE session_key NOT IN {tuple(recent_keys)}"
            db.execute_query(query)
    except Exception as e:
        print(f"Error cleaning up old sessions: {e}")
        all_success = False

    return all_success

def tableOfRaces():
    sessions_df = api.get_dataframe('sessions', {
    'year': time.localtime().tm_year,
    'session_type': 'Race' 
    })

    # Display last 5 races
    if not sessions_df.empty:
        sessions_df = sessions_df.sort_values('date_start')
        recent_sessions = sessions_df.tail(5)
        print("Recent F1 Races:")
        print("=" * 60)
        for _, session in recent_sessions.iterrows():
            print(f"{session['country_name']} GP - {session['location']}")
            print(f"   Session Key: {session['session_key']}")
            print(f"   Date: {session['date_start'][:10]}")
            print()

    return recent_sessions

def get_track_layout(session_key):
    """
    Fetches x, y coordinates for the entire race of the driver with the most laps.
    This ensures we capture the Pit Lane (In/Out laps) as well as the main track.
    """
    if not db.test_db_connection():
        return pd.DataFrame()

    # 1. Find the driver who completed the MOST laps
    # We assume the driver with the most laps likely pitted and finished the race.
    driver_query = f"""
    SELECT driver_number 
    FROM race_telemetry 
    WHERE session_key = {session_key} 
    GROUP BY driver_number 
    ORDER BY MAX(lap_number) DESC 
    LIMIT 1
    """
    driver_df = db.load_from_db(driver_query)
    
    if driver_df.empty:
        return None

    target_driver = driver_df.iloc[0]['driver_number']

    # 2. Get X, Y coordinates for ALL laps for that driver
    # We order by timestamp to ensure the line draws sequentially without jumping
    track_query = f"""
    SELECT x, y 
    FROM race_telemetry 
    WHERE session_key = {session_key} 
    AND driver_number = {target_driver}
    ORDER BY timestamp ASC
    """
    
    # 3. Load Data
    track_df = db.load_from_db(track_query)
    
    # OPTIONAL OPTIMIZATION:
    # Since we are drawing 50+ laps on top of each other, the dataframe might be huge (~200k rows).
    # We can downsample it to make the map generation instant while keeping the shape.
    # Taking every 5th point is usually enough for a high-res visual map.
    if len(track_df) > 10000:
        track_df = track_df.iloc[::5, :]
        
    return track_df

def plot_track_map(track_df):
    """
    Generates a minimalist, low-profile Matplotlib figure of the track.
    """
    if track_df is None or track_df.empty:
        return None
        
    # prevents the map from pushing the card content down.
    fig, ax = plt.subplots(figsize=(4, 1.5), dpi=100)
    
    # Plot the line
    ax.plot(track_df['x'], track_df['y'], color='#FF1801', linewidth=2)
    
    # Remove all axes, borders, and whitespace
    ax.axis('off')
    ax.set_aspect('equal', 'datalim') # Keeps track proportions correct
    
    # Remove all margins so the track touches the edges of the image
    fig.subplots_adjust(left=0, right=1, bottom=0, top=1)
    
    # Transparent background
    fig.patch.set_alpha(0) 
    
    return fig


# ---------------------------
# Get race replay data
# ---------------------------
def get_race_replay_data(session_key):
    """
    Fetches data and aligns drivers to the nearest second.
    """
    if not db.test_db_connection():
        return pd.DataFrame()

    # Fetch Raw Data
    query = f"""
    SELECT 
        driver_number,
        driver_acronym, 
        timestamp,
        x, 
        y, 
        lap_duration,
        lap_number 
    FROM race_telemetry 
    WHERE session_key = {session_key} 
    AND lap_number >= 2 
    ORDER BY timestamp ASC
    """
    
    df = db.load_from_db(query)
    
    if df.empty:
        return df

    # Convert Timestamp to correct format
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='ISO8601', errors='coerce')

    # Round timestamps to the nearest second for better track drawing sync
    # This forces NOR with ..32.722 and VER with ..32.850 both into "17:04:33" which is close enough for visual purposes
    df['timestamp_bucket'] = df['timestamp'].dt.round('1s')

    # Group by Driver + Time Bucket
    # We average X/Y just in case a driver has 2 points in the same second due to high sampling rate
    df_resampled = (
        df.groupby(['driver_acronym', 'driver_number', 'timestamp_bucket'])
        [['x', 'y', 'lap_duration', 'lap_number']]
        .mean()
        .reset_index()
    )

    # Create Race Time Integer seconds for the Laps Slider which will be converted to min:sec and/or laps later
    start_time = df_resampled['timestamp_bucket'].min()
    df_resampled['race_time'] = (df_resampled['timestamp_bucket'] - start_time).dt.total_seconds().astype(int)

    # Format & Return
    # Rename bucket back to timestamp for clarity
    df_resampled.rename(columns={'timestamp_bucket': 'timestamp'}, inplace=True)
    
    # Ensure integers for clean display
    df_resampled['lap_number'] = df_resampled['lap_number'].astype(int)
    df_resampled['driver_number'] = df_resampled['driver_number'].astype(int)

    return df_resampled

# ---------------------------
# Get driver details
# ---------------------------
def get_driver_colors(session_key):
    """
    Fetches driver colors using the specific API method requested.
    Includes edge case handling if API is unavailable.
    """
    default_color = "#FF1508" # Standard Formula 1 Red as fallback
    
    try:
        # 1. Try to fetch data using your specific syntax
        drivers = api.get_dataframe('drivers', {'session_key': session_key})
        
        # 2. Check if data is valid
        if drivers.empty:
            return pd.DataFrame(columns=['driver_acronym', 'team_colour'])

        # 3. Process Colors (API usually returns '3671C6', we need '#3671C6')
        # We also rename 'name_acronym' to 'driver_acronym' to match your telemetry data
        drivers['team_colour'] = drivers.get('team_colour').apply(lambda x: f"#{x}" if x else default_color)

        # Try to include a team name/constructor if available in the API response
        # Common possible column names: 'team_name', 'constructor', 'constructor_name'
        team_col = None
        for candidate in ('team_name', 'constructor', 'constructor_name'):
            if candidate in drivers.columns:
                team_col = candidate
                break

        if team_col is None:
            # Fallback to an empty string so callers can rely on the column existing
            drivers['team_name'] = ''
        else:
            drivers['team_name'] = drivers[team_col]

        # Select only what we need
        return drivers[['name_acronym', 'team_colour', 'team_name']].rename(columns={'name_acronym': 'driver_acronym'})

    except Exception as e:
        # EDGE CASE: API Unavailable, Network Error, or 'api' module missing
        print(f"API Error (Using default colors): {e}")
        return pd.DataFrame(columns=['driver_acronym', 'team_colour'])

if __name__ == "__main__":
    # For testing purposes
    #print(get_race_replay_data(9858))
    pass