import pandas as pd
import openf1_helper as of1

# Initialize API helper
api = of1.api

def get_weather_data(session_key):
    """
    Fetches weather data for the entire session and prepares it for merging.
    """
    print(f"Fetching weather data for session {session_key}...")
    
    try:
        # Fetch full session weather (no filters to ensure full timeline)
        weather_df = api.get_dataframe('weather', {'session_key': session_key})
        
        if weather_df.empty:
            print("Warning: No weather data found.")
            return pd.DataFrame()

        # 1. Convert timestamp to datetime (Crucial for merging)
        weather_df['date'] = pd.to_datetime(weather_df['date'])

        # 2. Sort by date (Required for merge_asof)
        weather_df = weather_df.sort_values('date')

        # 3. Select only columns relevant to the Predictive Model (Phase 2)
        # rainfall & track_temperature are key for tyre strategy
        keep_cols = ['date', 'rainfall', 'air_temperature', 'track_temperature', 'humidity']
        
        # Filter columns that actually exist in response
        final_cols = [c for c in keep_cols if c in weather_df.columns]
        
        print(f"Fetched {len(weather_df)} weather records.")
        return weather_df[final_cols]

    except Exception as e:
        print(f"Error fetching weather: {e}")
        return pd.DataFrame()