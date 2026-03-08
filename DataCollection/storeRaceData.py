import asyncio
import os
import time
import aiohttp
import pandas as pd
import datetime
from datetime import timedelta
import random
import math
import openf1_helper as of1
import sys
from pathlib import Path

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'DatabaseConnection')))
import databaseManager as db
import matplotlib.pyplot as plt

api = of1.api
SESSION_KEY = None

# --- ASYNC HTTP REQUEST HELPER ---
# Rate limiting for async requests
_last_async_request_time = 0
_async_request_lock = asyncio.Lock()

async def fetch(session, url, params, max_retries=5):
    """
    Fetches data from API with exponential backoff retry logic.
    
    Implements rate limit handling (429) and server error recovery.
    
    Args:
        session (aiohttp.ClientSession): Active HTTP session.
        url (str): API endpoint URL.
        params (dict): Query parameters.
        max_retries (int): Maximum retry attempts. Default: 5.
        
    Returns:
        list: JSON response data, or empty list if all retries fail.
    """
    global _last_async_request_time
    
    # Rate limiting: ensure minimum 300ms between async requests
    async with _async_request_lock:
        elapsed = time.time() - _last_async_request_time
        if elapsed < 0.3:
            await asyncio.sleep(0.3 - elapsed)
        _last_async_request_time = time.time()
    
    for attempt in range(max_retries):
        try:
            timeout = aiohttp.ClientTimeout(total=60)
            async with session.get(url, params=params, timeout=timeout) as response:
                if response.status == 429:
                    wait = (2 ** attempt) + random.uniform(2, 4)
                    await asyncio.sleep(wait)
                    continue
                if response.status >= 500:
                    wait = 5
                    await asyncio.sleep(wait)
                    continue
                    
                response.raise_for_status()
                return await response.json()
        except aiohttp.ClientError as e:
            wait = (2 ** attempt) + random.uniform(1, 2)
            await asyncio.sleep(wait)
    
    return []

# --- DRIVER & LAP DATA FETCHING ---
async def get_drivers():
    """
    Fetches all drivers for the session.
    
    Returns:
        list: List of tuples (driver_acronym, driver_number).
    """
    df = api.get_dataframe('drivers', {'session_key': SESSION_KEY})
    if df.empty:
        return []
    return [(row['name_acronym'], row['driver_number']) for _, row in df.iterrows()]

async def get_laps(driver_number):
    """
    Fetches lap data for a specific driver in chronological order.
    
    Args:
        driver_number (int): Driver ID.
        
    Returns:
        list: List of lap records sorted by date.
    """
    laps_df = api.get_dataframe('laps', {'session_key': SESSION_KEY, 'driver_number': driver_number})
    if laps_df.empty:
        return []
    laps_df = laps_df.sort_values('date_start')
    return laps_df.to_dict('records')

# --- LOCATION DATA FETCHING ---
async def get_locations(session, driver_number, start_iso, end_iso):
    """
    Fetches GPS location data for a driver within a time range.
    
    Args:
        session (aiohttp.ClientSession): Active HTTP session.
        driver_number (int): Driver ID.
        start_iso (str): ISO 8601 start timestamp.
        end_iso (str): ISO 8601 end timestamp.
        
    Returns:
        list: Location records with x, y, z coordinates.
    """
    params = {
        'session_key': SESSION_KEY,
        'driver_number': driver_number,
        'date>': start_iso,
        'date<': end_iso
    }
    url = "https://api.openf1.org/v1/location"
    return await fetch(session, url, params)

async def process_driver(driver_tuple, semaphore):
    """
    Fetches lap locations for a single driver with rate limiting.
    
    Splits data collection into 30-minute chunks to avoid API overload.
    Assigns GPS coordinates to their corresponding lap numbers.
    
    Args:
        driver_tuple (tuple): (driver_acronym, driver_number).
        semaphore (asyncio.Semaphore): Controls concurrent requests.
        
    Returns:
        list: Telemetry records for the driver.
    """
    acronym, driver_number = driver_tuple
    records = []

    laps = await get_laps(driver_number)
    if not laps:
        return records

    laps = [lap for lap in laps if lap.get('date_start')]
    if not laps:
        return records
    
    last_lap_duration = laps[-1].get('lap_duration')
    if last_lap_duration is None or (isinstance(last_lap_duration, float) and math.isnan(last_lap_duration)):
        last_lap_duration = 0.0

    start_time = pd.to_datetime(laps[0]['date_start'])
    end_time = pd.to_datetime(laps[-1]['date_start']) + timedelta(seconds=last_lap_duration)

    chunk_size = timedelta(minutes=30)
    current_start = start_time
    all_locs = []

    async with aiohttp.ClientSession() as session:
        async with semaphore:
            while current_start < end_time:
                current_end = min(current_start + chunk_size, end_time)
                
                chunk_data = await get_locations(
                    session, 
                    driver_number, 
                    current_start.isoformat(), 
                    current_end.isoformat()
                )
                all_locs.extend(chunk_data)
                
                current_start = current_end
                await asyncio.sleep(0.5)

    if not all_locs:
        return records

    locs_df = pd.DataFrame(all_locs)
    locs_df = locs_df.drop_duplicates(subset=['date'])
    locs_df['date'] = pd.to_datetime(locs_df['date'], format='ISO8601', errors='coerce')

    for lap in laps:
        lap_start = pd.to_datetime(lap['date_start'])
        lap_duration = lap.get('lap_duration')
        if lap_duration is None or (isinstance(lap_duration, float) and math.isnan(lap_duration)):
            lap_duration = 0.0
        lap_end = lap_start + timedelta(seconds=lap_duration)

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

    return records

# --- ASYNC DATA COLLECTION ---
async def fetchWithAPI():
    """
    Main async pipeline: fetches drivers, then processes each driver concurrently.
    
    Returns:
        pd.DataFrame: Combined telemetry data (location, timestamp, lap info).
    """
    drivers = await get_drivers()
    all_records = []

    semaphore = asyncio.Semaphore(1)  # Process one driver at a time to avoid rate limiting
    tasks = [process_driver(d, semaphore) for d in drivers]
    for future in asyncio.as_completed(tasks):
        result = await future
        all_records.extend(result)

    if not all_records:
        return None

    df = pd.DataFrame(all_records)
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='ISO8601', errors='coerce')
    df = df.sort_values('timestamp')
    
    return df

# --- DATABASE OPERATIONS ---
def fetchFromDB(session_key):
    """
    Fetches telemetry data from local database.
    
    Args:
        session_key (int): Session identifier.
        
    Returns:
        pd.DataFrame: Telemetry data for the session.
    """
    return db.load_from_db(f"SELECT * FROM race_telemetry WHERE session_key = {session_key}")

def updateDB():
    """
    Checks if session data exists in database. Fetches from API if missing.
    
    Returns:
        bool: True if data is available (cached or fetched), False otherwise.
    """
    try:
        existing = db.load_from_db(f"SELECT * FROM race_telemetry WHERE session_key = {SESSION_KEY}")
        
        if not existing.empty:
            return True

        df = asyncio.run(fetchWithAPI())
        
        if df is None or df.empty:
            return False

        db.save_to_db(df, 'race_telemetry', if_exists='append')
        return True
            
    except Exception as e:
        return False

def check_and_update_DB(session_key):
    """
    Sets session context and updates database.
    
    Args:
        session_key (int): Session identifier.
        
    Returns:
        bool: True if data is available, False otherwise.
    """
    global SESSION_KEY
    SESSION_KEY = session_key
    
    if not db.test_db_connection():
        return False
    
    return updateDB()

# --- SEASON & SESSION HANDLING ---
def get_season_year(today=None, season_start_month=3):
    """
    Returns the current F1 season year. Assumes season starts in March.
    
    For dates before March, returns the previous calendar year.
    
    Args:
        today (datetime, optional): Reference date. Defaults to current UTC time.
        season_start_month (int): Month when season starts. Default: 3 (March).
        
    Returns:
        int: Season year.
    """
    if today is None:
        today = datetime.datetime.now(datetime.timezone.utc)
    return today.year if today.month >= season_start_month else today.year - 1

def update_last_five_sessions():
    """
    Fetches and stores telemetry for the last 5 completed race sessions.
    Uses tableOfRaces() to ensure telemetry matches exactly what appears in the dashboard menu.
    
    Returns:
        bool: True if at least one session processed successfully, False otherwise.
    """
    # Get the exact same races that appear in the dashboard menu
    recent_sessions = tableOfRaces()
    
    if recent_sessions.empty:
        return False
    
    success_count = 0
    for _, session in recent_sessions.iterrows():
        if check_and_update_DB(session['session_key']):
            success_count += 1

    # Keep telemetry ONLY for these 5 races - delete everything else
    recent_keys = recent_sessions['session_key'].tolist()
    try:
        if len(recent_keys) == 1:
            keys_str = f"({recent_keys[0]})"
            db.execute_query(f"DELETE FROM race_telemetry WHERE session_key NOT IN {keys_str}")
        elif len(recent_keys) > 1:
            db.execute_query(f"DELETE FROM race_telemetry WHERE session_key NOT IN {tuple(recent_keys)}")
    except Exception:
        pass

    return success_count > 0

def tableOfRaces():
    """
    Fetches the last 5 completed races from the current or previous seasons.
    Searches backwards up to 3 years to ensure data availability.
    
    Returns:
        pd.DataFrame: Recent race session data.
    """
    current_year = datetime.datetime.now().year
    sessions_df = pd.DataFrame()  # Accumulate races across years
    
    # Collect completed races from current and previous years
    for year in range(current_year, current_year - 3, -1):
        try:
            year_sessions = api.get_dataframe('sessions', {
                'year': year,
                'session_type': 'Race'
            })
            
            if not year_sessions.empty:
                today = datetime.datetime.now().isoformat()
                completed = year_sessions[year_sessions['date_start'] < today].copy()
                
                if not completed.empty:
                    # Concatenate instead of returning immediately
                    sessions_df = pd.concat([sessions_df, completed])
                    
                    # If we have enough races, stop searching
                    if len(sessions_df) >= 5:
                        break
        except Exception:
            continue

    if sessions_df.empty:
        return pd.DataFrame()
    
    # Sort all races and return the 5 most recent
    sessions_df = sessions_df.sort_values('date_start', ascending=True)
    return sessions_df.tail(5)

# --- TRACK LAYOUT & VISUALIZATION ---
def get_track_layout(session_key):
    """
    Fetches track coordinates from the driver who completed the most laps.
    Ensures pit lane geometry is included alongside the main track.
    
    Args:
        session_key (int): Session identifier.
        
    Returns:
        pd.DataFrame: X, Y coordinates of the complete track layout.
    """
    if not db.test_db_connection():
        return pd.DataFrame()

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
        return pd.DataFrame()

    target_driver = driver_df.iloc[0]['driver_number']

    track_query = f"""
    SELECT x, y 
    FROM race_telemetry 
    WHERE session_key = {session_key} 
    AND driver_number = {target_driver}
    ORDER BY timestamp ASC
    """
    
    track_df = db.load_from_db(track_query)
    
    if len(track_df) > 10000:
        track_df = track_df.iloc[::5, :]
        
    return track_df

def plot_track_map(track_df):
    """
    Generates a minimalist track visualization for dashboard display.
    
    Args:
        track_df (pd.DataFrame): Track coordinates.
        
    Returns:
        matplotlib.figure.Figure: Track map figure with no axes or borders.
    """
    if track_df is None or track_df.empty:
        return None
        
    fig, ax = plt.subplots(figsize=(4, 1.5), dpi=100)
    
    ax.plot(track_df['x'], track_df['y'], color='#FF1801', linewidth=2)
    
    ax.axis('off')
    ax.set_aspect('equal', 'datalim')
    
    fig.subplots_adjust(left=0, right=1, bottom=0, top=1)
    fig.patch.set_alpha(0)
    
    return fig

# --- RACE REPLAY DATA ---
def get_race_replay_data(session_key):
    """
    Prepares telemetry data for animated race replay.
    
    Resamples driver positions to 1-second intervals and calculates correct lap times
    from lap start transitions. Filters out data from lap 1 (formation lap).
    
    Args:
        session_key (int): Session identifier.
        
    Returns:
        tuple: (resampled_positions_df, lap_times_df)
    """
    if not db.test_db_connection():
        return pd.DataFrame(), pd.DataFrame()

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
        return df, pd.DataFrame()

    df['timestamp'] = pd.to_datetime(df['timestamp'], format='ISO8601', errors='coerce')
    df = df.dropna(subset=['timestamp'])

    lap_start_times = df.groupby(['driver_acronym', 'lap_number'])['timestamp'].min().reset_index()
    lap_start_times.rename(columns={'timestamp': 'lap_start_time'}, inplace=True)
    laps_sorted = lap_start_times.sort_values(['driver_acronym', 'lap_number'])
    laps_sorted['lap_time'] = laps_sorted.groupby('driver_acronym')['lap_start_time'].diff().shift(-1)
    laps_sorted['lap_time'] = laps_sorted['lap_time'].dt.total_seconds()
    lap_times_df = laps_sorted.dropna(subset=['lap_time'])[['driver_acronym', 'lap_number', 'lap_time']]

    df['timestamp_bucket'] = df['timestamp'].dt.round('1s')

    df_resampled = (
        df.groupby(['driver_acronym', 'driver_number', 'timestamp_bucket'])
        [['x', 'y', 'lap_duration', 'lap_number']]
        .mean()
        .reset_index()
    )

    start_time = df_resampled['timestamp_bucket'].min()
    df_resampled['race_time'] = (df_resampled['timestamp_bucket'] - start_time).dt.total_seconds().astype(int)

    df_resampled.rename(columns={'timestamp_bucket': 'timestamp'}, inplace=True)
    
    df_resampled['lap_number'] = df_resampled['lap_number'].astype(int)
    df_resampled['driver_number'] = df_resampled['driver_number'].astype(int)
    
    return df_resampled, lap_times_df

# --- DRIVER METADATA ---
def get_driver_colors(session_key):
    """
    Fetches team colors and team names for all drivers in the session.
    
    Args:
        session_key (int): Session identifier.
        
    Returns:
        pd.DataFrame: Driver acronyms, team colors, and team names.
    """
    default_color = "#FF1508"
    
    try:
        drivers = api.get_dataframe('drivers', {'session_key': session_key})
        
        if drivers.empty:
            return pd.DataFrame(columns=['driver_acronym', 'team_colour', 'team_name'])

        drivers['team_colour'] = drivers.get('team_colour').apply(lambda x: f"#{x}" if x else default_color)
        
        team_col = None
        for candidate in ('team_name', 'constructor', 'constructor_name'):
            if candidate in drivers.columns:
                team_col = candidate
                break

        drivers['team_name'] = drivers[team_col] if team_col else ''

        return drivers[['name_acronym', 'team_colour', 'team_name']].rename(columns={'name_acronym': 'driver_acronym'})

    except Exception:
        return pd.DataFrame(columns=['driver_acronym', 'team_colour', 'team_name'])

# --- RACE CONTROL EVENTS ---
def get_safety_car_data(session_key):
    """
    Fetches safety car and VSC deployment events for the session.
    
    Args:
        session_key (int): Session identifier.
        
    Returns:
        pd.DataFrame: Race control events (flag, message, lap number).
    """
    df = api.get_dataframe('race_control', {'session_key': session_key})
    return df if not df.empty else pd.DataFrame()

if __name__ == "__main__":
    pass