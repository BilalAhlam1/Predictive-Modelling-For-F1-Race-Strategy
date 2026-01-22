import asyncio
import datetime
import os
import pandas as pd
import openf1_helper as of1
import weatherData as wd
import sys, os; sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'DatabaseConnection')))
import databaseManager as db

api = of1.api
session_key = None  # to be set when calling functions

# ---------------------------
# Helper: Get Tyre Info
# ---------------------------
def get_tyre_info(row, stints_df):
    """
    Calculates specific tyre compound and actual tyre life for a given lap
    by matching the driver and lap number to the stint range.
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
    else:
        return pd.Series([None, None])

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
# Lap Processor
# ---------------------------
async def process_driver(driver_tuple, semaphore):
    """Fetch all laps for one driver with basic retry logic."""
    acronym, driver_number = driver_tuple
    #print(f"Processing driver {acronym} ({driver_number})")

    # retry logic incase of transient failures
    for attempt in range(3):
        try:
            async with semaphore:
                # Add a tiny delay to ensure we don't hit rate limits even with the semaphore
                await asyncio.sleep(0.5) 
                laps = await get_laps(driver_number)
                
            # If successful, break the retry loop
            break 
        except Exception as e:
            if attempt < 2:
                print(f"Error fetching laps for {acronym}: {e}. Retrying...")
                await asyncio.sleep(2)
            else:
                print(f"Failed to fetch laps for {acronym} after 3 attempts.")
                return []

    if not laps:
        # It's normal for some reserve drivers to have 0 laps in a race
        # print(f"No laps found for driver {acronym} ({driver_number})") 
        return []

    # Filter out laps with no start time
    laps = [lap for lap in laps if lap.get("date_start")]

    print(f"Completed driver {acronym} ({driver_number}), fetched {len(laps)} laps")
    return laps

# ---------------------------
# Fetch drivers for the session
# ---------------------------
async def get_drivers():
    """Get list of drivers for the session with retry logic."""
    import asyncio
    
    for attempt in range(3):
        try:
            df = api.get_dataframe('drivers', {'session_key': SESSION_KEY})
            if not df.empty:
                return [(row['name_acronym'], row['driver_number']) for _, row in df.iterrows()]
            else:
                print("No drivers found in API response.")
                return []
        except Exception as e:
            if "429" in str(e):
                wait = (attempt + 1) * 2
                print(f"429 Too Many Requests (Drivers). Retrying in {wait}s...")
                await asyncio.sleep(wait)
            else:
                print(f"Error fetching drivers: {e}")
                break
                
    return []

# ---------------------------
# Main async runner
# ---------------------------
async def fetchWithAPI(session_key):
    # Fetch Stints & Weather
    print(f"Fetching auxiliary data for session {session_key}...")
    
    # Fetch Stints
    try:
        df_stints = api.get_dataframe('stints', {'session_key': session_key})
        #print(f"Fetched {len(df_stints)} stints.")
    except Exception as e:
        print(f"Error fetching stints: {e}")
        df_stints = pd.DataFrame()

    # Fetch Weather
    df_weather = wd.get_weather_data(session_key)

    # Setup Async Lap Fetching
    drivers = await get_drivers() 
    all_laps = []
    semaphore = asyncio.Semaphore(2)

    lap_tasks = [process_driver(d, semaphore) for d in drivers]
    
    print("Starting lap data collection")
    results = await asyncio.gather(*lap_tasks)

    for l_list in results:
        all_laps.extend(l_list)

    if not all_laps:
        print("No lap data collected")
        return

    # Convert Laps to DataFrame
    df_laps = pd.DataFrame(all_laps)

    # Apply Stint logic
    print("Mapping tyre data to laps")
    if not df_stints.empty:
        df_laps[['tire_compound', 'laps_on_tire']] = df_laps.apply(
            lambda row: get_tyre_info(row, df_stints), axis=1
        )
    else:
        df_laps['tire_compound'] = None
        df_laps['laps_on_tire'] = None

    # merge Weather Data
    print("Merging weather data")
    if not df_weather.empty and 'date_start' in df_laps.columns:
        # Ensure datetime format
        df_laps['date_start'] = pd.to_datetime(df_laps['date_start'], format='mixed')
        
        # Sort by time
        df_laps = df_laps.sort_values('date_start')
        
        # Find the weather record closest to the lap start time
        df_laps = pd.merge_asof(
            df_laps,
            df_weather,
            left_on='date_start',
            right_on='date',
            direction='nearest',
            tolerance=pd.Timedelta('5min') # limit match to within 5 mins for interpolation
        )
    else:
        print("Skipping weather merge (missing data).")

    # Select and Order Columns for ML
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

    # Convert List Columns to Strings for SQLite
    list_cols = ['segments_sector_1', 'segments_sector_2', 'segments_sector_3']
    for col in list_cols:
        if col in df_final.columns:
            # Convert list
            df_final[col] = df_final[col].astype(str)

    # Re-sort by Driver then Lap for readability
    df_final = df_final.sort_values(['driver_number', 'lap_number'])
    return df_final

def fetchMLData(session_key):
    global SESSION_KEY
    SESSION_KEY = session_key
    
    # Check if session data already exists in DB
    Sessions = db.load_from_db(f"""SELECT * FROM ml_training_data WHERE session_key = {session_key}""")
    
    if Sessions.empty:
        print(f"Session {session_key} not found in database.")
        df = asyncio.run(fetchWithAPI(session_key))
        
        # Check if df is valid before saving
        if df is not None and not df.empty:
            db.save_to_db(df, 'ml_training_data', if_exists='append')
            return df
        else:
            print(f"Failed to fetch ML data for session {session_key}")
            return pd.DataFrame() # Return empty DF instead of None
            
    else:
        #print(f"Session {session_key} found in database.")
        df = db.load_from_db(f"""SELECT * FROM ml_training_data WHERE session_key = {session_key}""")

    return df


def updateMLData(session_key):
    if db.test_db_connection():
        #print("Database connection successful.")
        df = fetchMLData(session_key)
        #print(f"Data ready with {len(df)} rows for session {session_key}.")
        return True
    else: 
        print("Database connection failed. Falling back to API fetch.")
        df = asyncio.run(fetchWithAPI(session_key))
        #print(f"Data ready with {len(df)} rows for session {session_key}.")
        return False
    
# ---------------------------
# get season year (adjust month if needed)
# ---------------------------
def get_season_year(today=None, season_start_month=3):
    """
    Return the season year to query.
    If the current month is before the season_start_month, return previous year.
    Default assumes season starts in March so Jan/Feb use previous year.
    """
    if today is None:
        today = datetime.datetime.now(datetime.timezone.utc)
    return today.year if today.month >= season_start_month else today.year - 1

# ---------------------------
# Update last five sessions and store if not present
# ---------------------------
def update_last_five_sessions():
    """
    Fetch the last five session keys and update DB.
    Iterates backwards through years to find valid completed races.
    """
    import datetime
    
    current_year = datetime.datetime.now().year
    
    sessions_df = pd.DataFrame()
    for year in range(current_year, current_year - 3, -1):
        try:
            temp_df = api.get_dataframe('sessions', {
                'year': year,
                'session_type': 'Race'
            })
            if not temp_df.empty:
                # Filter for completed races
                today = datetime.datetime.now().isoformat()
                completed = temp_df[temp_df['date_start'] < today]
                
                if not completed.empty:
                    sessions_df = completed
                    break # Found the most recent valid season
        except Exception as e:
            print(f"Error checking {year}: {e}")
            continue

    if sessions_df.empty:
        print("No sessions found in the last 3 years.")
        return False

    sessions_df = sessions_df.sort_values('date_start')
    recent_sessions = sessions_df.tail(5)
    
    all_success = True

    for _, session in recent_sessions.iterrows():
        result = updateMLData(session['session_key'])
        if not result:
            all_success = False

    # Cleanup old sessions
    recent_keys = sessions_df['session_key'].tail(5).tolist()
    try:
        if recent_keys:
            if len(recent_keys) == 1:
                keys_str = f"({recent_keys[0]})"
            else:
                keys_str = str(tuple(recent_keys))
            
            query = f"DELETE FROM race_telemetry WHERE session_key NOT IN {keys_str}"
            db.execute_query(query)
    except Exception as e:
        print(f"Error cleaning up old sessions: {e}")
        all_success = False

    return all_success

if __name__ == "__main__":
    # For testing purposes
    #print(get_season_year())
    update_last_five_sessions()
    pass