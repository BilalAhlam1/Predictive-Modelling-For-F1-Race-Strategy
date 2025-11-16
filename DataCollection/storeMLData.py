import asyncio
import os
import pandas as pd
import openf1_helper as of1
import storeRaceData as fetcher
import aiohttp

SESSION_KEY = fetcher.SESSION_KEY
output_folder = "RaceDataCSV"
os.makedirs(output_folder, exist_ok=True)
OUTPUT_CSV = os.path.join(output_folder, "MLData_session_{SESSION_KEY}.csv")
api = of1.api

async def process_driver(driver_tuple, semaphore):
    """Fetch all laps for one driver and return them exactly as JSON dictionaries."""
    acronym, driver_number = driver_tuple
    print(f"Processing driver {acronym} ({driver_number})")

    # Limit concurrency (if needed)
    async with semaphore:
        laps = await fetcher.get_laps(driver_number)

    if not laps:
        print(f"No laps found for driver {acronym} ({driver_number})")
        return []

    # Filter out bad entries that have no start time (e.g., DNF laps)
    laps = [lap for lap in laps if lap.get("date_start")]

    print(f"Completed driver {acronym} ({driver_number}), fetched {len(laps)} laps")
    return laps


# ---------------------------
# Main async runner
# ---------------------------
async def main():
    """Save all laps to a CSV file."""
    drivers = await fetcher.get_drivers() # List of (acronym, number)
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
    df = df.sort_values('lap_number')
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Saved all lap locations to {OUTPUT_CSV} ({len(df)} rows)")

# ---------------------------
# Run
# ---------------------------
if __name__ == "__main__":
    asyncio.run(main())
