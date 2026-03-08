import asyncio
import datetime
import os
import pandas as pd
import openf1_helper as of1
import weatherData as wd
import sys
from pathlib import Path

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'DatabaseConnection')))
import databaseManager as db

api = of1.api
SESSION_KEY = None

# --- HELPER FUNCTIONS ---
def get_tyre_info(row, stints_df):
    """
    Maps tyre compound and age to a lap by matching the driver and lap number
    to the corresponding stint range.
    
    Args:
        row (pd.Series): Lap data row.
        stints_df (pd.DataFrame): Stint data containing compound and tyre age.
        
    Returns:
        pd.Series: [compound, laps_on_tire] or [None, None] if no match found.
    """
    stint = stints_df[
        (stints_df['driver_number'] == row['driver_number']) &
        (stints_df['lap_start'] <= row['lap_number']) &
        (stints_df['lap_end'] >= row['lap_number'])
    ]
    
    if not stint.empty:
        s = stint.iloc[0]
        compound = s['compound']
        laps_on_tire = (row['lap_number'] - s['lap_start']) + s['tyre_age_at_start']
        return pd.Series([compound, laps_on_tire])
    
    return pd.Series([None, None])

# --- ASYNC DATA FETCHING ---
async def get_laps(driver_number):
    """
    Fetches lap data for a specific driver in the session.
    
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

async def get_drivers():
    """
    Fetches driver list for the session with retry logic (max 3 attempts).
    
    Returns:
        list: List of tuples (driver_acronym, driver_number).
    """
    for attempt in range(3):
        try:
            df = api.get_dataframe('drivers', {'session_key': SESSION_KEY})
            if not df.empty:
                return [(row['name_acronym'], row['driver_number']) for _, row in df.iterrows()]
        except Exception as e:
            if "429" in str(e) and attempt < 2:
                wait_time = (attempt + 1) * 2
                await asyncio.sleep(wait_time)
            else:
                break
    
    return []

async def process_driver(driver_tuple, semaphore):
    """
    Fetches laps for a single driver with retry logic and rate limiting.
    
    Args:
        driver_tuple (tuple): (driver_acronym, driver_number).
        semaphore (asyncio.Semaphore): Controls concurrent API requests.
        
    Returns:
        list: List of lap records for the driver.
    """
    acronym, driver_number = driver_tuple
    
    for attempt in range(3):
        try:
            async with semaphore:
                await asyncio.sleep(0.5)
                laps = await get_laps(driver_number)
                break
        except Exception as e:
            if attempt < 2:
                await asyncio.sleep(2)
            else:
                return []
    
    if not laps:
        return []
    
    laps = [lap for lap in laps if lap.get("date_start")]
    return laps

async def fetchWithAPI(session_key):
    """
    Main data collection pipeline: fetches laps, stints, and weather data,
    then merges them for machine learning training.
    
    Args:
        session_key (int): Session identifier.
        
    Returns:
        pd.DataFrame: Combined lap, tyre, and weather data ready for ML.
    """
    # Fetch stints (tyre strategy information)
    try:
        df_stints = api.get_dataframe('stints', {'session_key': session_key})
    except Exception as e:
        df_stints = pd.DataFrame()
    
    # Fetch weather data
    df_weather = wd.get_weather_data(session_key)
    
    # Fetch drivers and their laps concurrently
    drivers = await get_drivers()
    if not drivers:
        return None
    
    all_laps = []
    semaphore = asyncio.Semaphore(2)
    lap_tasks = [process_driver(d, semaphore) for d in drivers]
    results = await asyncio.gather(*lap_tasks)
    
    for lap_list in results:
        all_laps.extend(lap_list)
    
    if not all_laps:
        return None
    
    # Convert laps to DataFrame
    df_laps = pd.DataFrame(all_laps)
    
    # Map tyre compound and age to each lap
    if not df_stints.empty:
        df_laps[['tire_compound', 'laps_on_tire']] = df_laps.apply(
            lambda row: get_tyre_info(row, df_stints), axis=1
        )
    else:
        df_laps['tire_compound'] = None
        df_laps['laps_on_tire'] = None
    
    # Merge weather data by nearest timestamp
    if not df_weather.empty and 'date_start' in df_laps.columns:
        df_laps['date_start'] = pd.to_datetime(df_laps['date_start'], format='mixed')
        df_laps = df_laps.sort_values('date_start')
        
        df_laps = pd.merge_asof(
            df_laps,
            df_weather,
            left_on='date_start',
            right_on='date',
            direction='nearest',
            tolerance=pd.Timedelta('5min')
        )
    
    # Select and order columns for ML training
    desired_columns = [
        'meeting_key', 'session_key', 'driver_number', 'lap_number',
        'date_start', 'lap_duration',
        'duration_sector_1', 'duration_sector_2', 'duration_sector_3',
        'st_speed', 'i1_speed', 'i2_speed',
        'segments_sector_1', 'segments_sector_2', 'segments_sector_3',
        'is_pit_out_lap',
        'tire_compound', 'laps_on_tire',
        'rainfall', 'track_temperature', 'air_temperature', 'humidity'
    ]
    
    final_cols = [c for c in desired_columns if c in df_laps.columns]
    df_final = df_laps[final_cols].copy()
    
    # Convert list columns to strings for SQLite compatibility
    list_cols = ['segments_sector_1', 'segments_sector_2', 'segments_sector_3']
    for col in list_cols:
        if col in df_final.columns:
            df_final[col] = df_final[col].astype(str)
    
    df_final = df_final.sort_values(['driver_number', 'lap_number'])
    return df_final

# --- DATABASE OPERATIONS ---
def fetchMLData(session_key):
    """
    Fetches ML training data for a session. Returns cached data if available,
    otherwise fetches from API and stores in database.
    
    Args:
        session_key (int): Session identifier.
        
    Returns:
        pd.DataFrame: ML training data for the session.
    """
    global SESSION_KEY
    SESSION_KEY = session_key
    
    # Check if session data exists in database
    cached_data = db.load_from_db(f"SELECT * FROM ml_training_data WHERE session_key = {session_key}")
    
    if not cached_data.empty:
        return cached_data
    
    # Fetch from API if not cached
    df = asyncio.run(fetchWithAPI(session_key))
    
    if df is not None and not df.empty:
        db.save_to_db(df, 'ml_training_data', if_exists='append')
        return df
    
    return pd.DataFrame()

def updateMLData(session_key):
    """
    Updates ML training data for a session by fetching from API.
    
    Args:
        session_key (int): Session identifier.
        
    Returns:
        bool: True if database connection successful, False otherwise.
    """
    if db.test_db_connection():
        fetchMLData(session_key)
        return True
    else:
        asyncio.run(fetchWithAPI(session_key))
        return False

# --- SEASON & CURRENT DATA ---
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
    Fetches and updates ML training data for the last 5 completed race sessions.
    Uses the same race list as storeRaceData to keep ML data in sync with telemetry.
    
    Returns:
        bool: True if at least one session updated successfully, False otherwise.
    """
    current_year = datetime.datetime.now().year
    sessions_df = pd.DataFrame()
    
    # Search for completed races in the last 3 years - ACCUMULATE across years
    for year in range(current_year, current_year - 3, -1):
        try:
            temp_df = api.get_dataframe('sessions', {
                'year': year,
                'session_type': 'Race'
            })
            
            if not temp_df.empty:
                today = datetime.datetime.now().isoformat()
                completed = temp_df[temp_df['date_start'] < today]
                
                if not completed.empty:
                    # Concatenate
                    sessions_df = pd.concat([sessions_df, completed])
                    
                    # Stop if we have enough races
                    if len(sessions_df) >= 5:
                        break
        except Exception:
            continue
    
    if sessions_df.empty:
        return False
    
    # Update the 5 most recent sessions
    sessions_df = sessions_df.sort_values('date_start')
    recent_sessions = sessions_df.tail(5)
    
    success_count = 0
    for _, session in recent_sessions.iterrows():
        print(f"Updating ML data for session {session['session_key']}...")
        if updateMLData(session['session_key']):
            success_count += 1
    
    # Clean up ML training data for old sessions
    recent_keys = recent_sessions['session_key'].tolist()
    try:
        if len(recent_keys) == 1:
            keys_str = f"({recent_keys[0]})"
            db.execute_query(f"DELETE FROM ml_training_data WHERE session_key NOT IN {keys_str}")
        elif len(recent_keys) > 1:
            db.execute_query(f"DELETE FROM ml_training_data WHERE session_key NOT IN {tuple(recent_keys)}")
    except Exception:
        pass
    
    return success_count > 0

if __name__ == "__main__":
    update_last_five_sessions()