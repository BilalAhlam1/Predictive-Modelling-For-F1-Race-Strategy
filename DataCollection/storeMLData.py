import asyncio
import os
import pandas as pd
import openf1_helper as of1
import storeRaceData as fetcher
import weatherData as wd
import sys, os; sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'DatabaseConnection')))
import databaseManager as db

SESSION_KEY = fetcher.SESSION_KEY
api = of1.api

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
# Lap Processor
# ---------------------------
async def process_driver(driver_tuple, semaphore):
    """Fetch all laps for one driver with basic retry logic."""
    acronym, driver_number = driver_tuple
    print(f"Processing driver {acronym} ({driver_number})")

    # retry logic incase of transient failures
    for attempt in range(3):
        try:
            async with semaphore:
                # Add a tiny delay to ensure we don't hit rate limits even with the semaphore
                await asyncio.sleep(0.5) 
                laps = await fetcher.get_laps(driver_number)
                
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
# Main async runner
# ---------------------------
async def fetchWithAPI():
    # Fetch Stints & Weather
    print(f"Fetching auxiliary data for session {SESSION_KEY}...")
    
    # Fetch Stints
    try:
        df_stints = api.get_dataframe('stints', {'session_key': SESSION_KEY})
        print(f"Fetched {len(df_stints)} stints.")
    except Exception as e:
        print(f"Error fetching stints: {e}")
        df_stints = pd.DataFrame()

    # Fetch Weather
    df_weather = wd.get_weather_data(SESSION_KEY)

    # Setup Async Lap Fetching
    drivers = await fetcher.get_drivers() 
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

def fetchWithDB():
    "# Check if session data already exists in DB, otherwise run data collection"
    Sessions = db.load_from_db(f"""SELECT * FROM ml_training_data WHERE session_key = {SESSION_KEY}""")
    if Sessions.empty:
        print(f"Session {SESSION_KEY} not found in database.")
        df = asyncio.run(fetchWithAPI())
        db.save_to_db(df, 'ml_training_data', if_exists='append')
    else:
        print(f"Session {SESSION_KEY} found in database.")
        df = db.load_from_db(f"""SELECT * FROM ml_training_data WHERE session_key = {SESSION_KEY}""")

    return df


if __name__ == "__main__":
    if db.test_db_connection():
        print("Database connection successful.")
        df = fetchWithDB()
        print(f"Data ready with {len(df)} rows for session {SESSION_KEY}.")
    else: 
        print("Database connection failed. Falling back to API fetch.")
        df = asyncio.run(fetchWithAPI())
        print(f"Data ready with {len(df)} rows for session {SESSION_KEY}.")

        