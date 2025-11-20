import os
import pandas as pd
import numpy as np
import time
from sqlalchemy import create_engine, text
import sys

# Add paths for custom modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'DataCollection')))
import openf1_helper as of1
import storeMLData as ml
import storeRaceData as rd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'DatabaseConnection')))
import databaseManager as db

SESSION_KEY = rd.SESSION_KEY

def validate_data(db_df, api_df):
    """
    Validates that the data fetched from the database matches the data fetched from the API.
    """
    print("\nStarting Data Validation")

    #Handle NoneType if API fetch failed completely
    if api_df is None:
        print("Validation Aborted: API DataFrame is None (Likely due to 429 Rate Limit errors).")
        return False

    if db_df.empty:
        print("Database DataFrame is empty.")
        return False
    if api_df.empty:
        print("API DataFrame is empty.")
        return False

    # --- 1. Check for Missing Drivers ---
    db_drivers = set(db_df['driver_number'].unique())
    api_drivers = set(api_df['driver_number'].unique())
    
    missing_in_api = db_drivers - api_drivers
    missing_in_db = api_drivers - db_drivers
    
    if missing_in_api:
        print(f"Drivers in DB but missing from API fetch: {missing_in_api}")
        print("Excluding these drivers from validation.")
    
    if missing_in_db:
        print(f"Drivers in API but missing from DB: {missing_in_db}")
        return False

    common_drivers = db_drivers.intersection(api_drivers)
    if not common_drivers:
        print("No common drivers found between DB and API. Validation impossible.")
        return False
        
    db_df_filtered = db_df[db_df['driver_number'].isin(common_drivers)].copy()
    api_df_filtered = api_df[api_df['driver_number'].isin(common_drivers)].copy()

    # Compare Row Counts
    rows_db = len(db_df_filtered)
    rows_api = len(api_df_filtered)
    
    if rows_db != rows_api:
        print(f"Row count mismatch for common drivers: DB({rows_db}) vs API({rows_api})")
        return False
    else:
        print(f"Row counts match for {len(common_drivers)} drivers: {rows_db} rows.")

    # Compare Key Statistics
    numeric_cols = api_df_filtered.select_dtypes(include='number').columns
    validation_passed = True
    
    print("Verifying numerical statistics")
    for col in numeric_cols:
        if col not in db_df_filtered.columns:
            print(f"Column '{col}' found in API data but missing in DB data.")
            continue
            
        db_mean = db_df_filtered[col].mean()
        api_mean = api_df_filtered[col].mean()
        
        if not np.isclose(db_mean, api_mean, rtol=1e-5, equal_nan=True):
            print(f"Mean mismatch in column '{col}': DB({db_mean:.4f}) vs API({api_mean:.4f})")
            validation_passed = False

    if validation_passed:
        print("Validation successful: Database data integrity confirmed against API.")
        return True
    else:
        print("Validation failed: Statistics mismatches found.")
        return False

def store():
    "# Store ML Data with DB Integration"
    if db.test_db_connection():
        print("Database connection successful.")
        try:
            df = ml.fetchWithDB() 
        except AttributeError:
            print("Error: fetchWithDB function not found in storeMLData module.")
            return
            
        print(f"Data ready with {len(df)} rows for session {SESSION_KEY}.")
    else: 
        print("Database connection failed. Falling back to API fetch.")
        df = ml.asyncio.run(ml.fetchWithAPI())
        print(f"Data ready with {len(df)} rows for session {SESSION_KEY}.")

if __name__ == "__main__":
    # Replace ml with rd to store race telemetry data instead and vice versa

    # Run the storage logic
    store()
    
    # Load the data from DB
    print(f"\nLoading Data from Database for Session {SESSION_KEY}")
    # Ensure table name matches what is saved to the table
    db_df = db.load_from_db(f"SELECT * FROM ml_training_data WHERE session_key = {SESSION_KEY}")
    
    # Add Cool-down to prevent 429 Errors
    print("\nCooling down API for 10 seconds before verification fetch...")
    time.sleep(10) 
    
    # Fetch fresh data from API
    print(f"--- Fetching Fresh Data from API for Verification ---")
    api_df = ml.asyncio.run(ml.fetchWithAPI())
    
    # Validate
    validate_data(db_df, api_df)