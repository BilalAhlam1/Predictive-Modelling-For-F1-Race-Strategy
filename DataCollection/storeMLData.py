import asyncio
import os
import pandas as pd
import openf1_helper as of1
import storeRaceData as fetcher

SESSION_KEY = fetcher.SESSION_KEY
output_folder = "RaceDataCSV"
os.makedirs(output_folder, exist_ok=True)
OUTPUT_CSV = os.path.join(output_folder, f"MLData_session_{SESSION_KEY}.csv")
api = of1.api

# ---------------------------
# Helper: Get Tyre Info
# ---------------------------
def get_tyre_info(row, stints_df):
    """
    Calculates specific tyre compound and actual tyre life for a given lap
    by matching the driver and lap number to the stint range.
    """
    # Find the stint where:
    # 1. Driver matches
    # 2. Lap is between Start and End of the stint
    stint = stints_df[
        (stints_df['driver_number'] == row['driver_number']) &
        (stints_df['lap_start'] <= row['lap_number']) &
        (stints_df['lap_end'] >= row['lap_number'])
    ]
    
    if not stint.empty:
        s = stint.iloc[0]
        compound = s['compound']
        # (Current Lap - Stint Start) + Laps already on tyre at start
        laps_on_tire = (row['lap_number'] - s['lap_start']) + s['tyre_age_at_start']
        return pd.Series([compound, laps_on_tire])
    else:
        return pd.Series([None, None])

# ---------------------------
# Lap Processor
# ---------------------------
async def process_driver(driver_tuple, semaphore):
    """Fetch all laps for one driver."""
    acronym, driver_number = driver_tuple
    print(f"Processing driver {acronym} ({driver_number})")

    async with semaphore:
        laps = await fetcher.get_laps(driver_number)

    if not laps:
        print(f"No laps found for driver {acronym} ({driver_number})")
        return []

    # Filter out laps with no start time (invalid laps)
    laps = [lap for lap in laps if lap.get("date_start")]

    print(f"Completed driver {acronym} ({driver_number}), fetched {len(laps)} laps")
    return laps

# ---------------------------
# Main async runner
# ---------------------------
async def main():
    """Fetch laps (async) and stints (sync), merge them, and save specific ML columns."""
    
    # 1. Fetch Stints Synchronously using the helper
    # runs before the async loop starts
    print(f"Fetching stints for session {SESSION_KEY}...")
    try:
        # Using the clean helper method to get stints as DataFrame
        df_stints = api.get_dataframe('stints', {'session_key': SESSION_KEY})
        print(f"Fetched {len(df_stints)} stints.")
    except Exception as e:
        print(f"Error fetching stints: {e}")
        df_stints = pd.DataFrame()

    # 2. Setup Async Lap Fetching
    drivers = await fetcher.get_drivers() 
    all_laps = []
    semaphore = asyncio.Semaphore(2)

    lap_tasks = [process_driver(d, semaphore) for d in drivers]
    
    print("Starting lap data collection...")
    # only wait for lap tasks now, as stints are already loaded
    results = await asyncio.gather(*lap_tasks)

    # Flatten the list of lists
    for l_list in results:
        all_laps.extend(l_list)

    if not all_laps:
        print("No lap data collected.")
        return

    # 3. Convert Laps to DataFrame
    df_laps = pd.DataFrame(all_laps)

    # 4. Apply the Stint logic to get Tyre info
    print("Mapping tyre data to laps...")
    if not df_stints.empty:
        df_laps[['tire_compound', 'laps_on_tire']] = df_laps.apply(
            lambda row: get_tyre_info(row, df_stints), axis=1
        )
    else:
        # Fallback if no stint data found
        df_laps['tire_compound'] = None
        df_laps['laps_on_tire'] = None

    # 5. Select and Order Columns for ML
    desired_columns = [
        'meeting_key', 'session_key', 'driver_number', 'lap_number', 
        'date_start', 'lap_duration', 
        'duration_sector_1', 'duration_sector_2', 'duration_sector_3', 
        'st_speed', 'i1_speed', 'i2_speed', 
        'segments_sector_1', 'segments_sector_2', 'segments_sector_3',
        'is_pit_out_lap',
        'tire_compound', 'laps_on_tire'
    ]
    
    # Intersection of desired vs actual to prevent KeyErrors
    final_cols = [c for c in desired_columns if c in df_laps.columns]
    df_final = df_laps[final_cols]

    df_final = df_final.sort_values(['driver_number', 'lap_number'])
    df_final.to_csv(OUTPUT_CSV, index=False)
    
    print(f"Saved processed ML data to {OUTPUT_CSV} ({len(df_final)} rows)")

if __name__ == "__main__":
    asyncio.run(main())