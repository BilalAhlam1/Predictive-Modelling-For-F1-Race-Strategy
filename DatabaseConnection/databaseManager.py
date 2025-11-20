import os
import pandas as pd
from sqlalchemy import create_engine, text

# --- CONFIGURATION ---
# 1. Define the base directory for the database
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 2. Ensure the directory exists
os.makedirs(BASE_DIR, exist_ok=True)

# 3. Database file path and URL
DB_NAME = "f1_strategy.db"
DB_PATH = os.path.join(BASE_DIR, DB_NAME)
DB_URL = f"sqlite:///{DB_PATH}"

print(f"💻 DATA MANAGER: Database locked to: {DB_PATH}")

# --- DATABASE ENGINE ---
engine = create_engine(DB_URL)

# --- HELPER FUNCTIONS ---

def save_to_db(df, table_name, if_exists='append'):
    """
    Saves a Pandas DataFrame to the local database.
    if_exists options:
      - 'append': Add new rows (Default)
      - 'replace': Delete table and write new data
      - 'fail': Do nothing if table exists
    """
    if df.empty:
        print(f"No data to save for {table_name}")
        return

    try:
        # 'begin' automatically handles the transaction commit
        with engine.begin() as conn:
            df.to_sql(table_name, conn, if_exists=if_exists, index=False)
            print(f"Saved {len(df)} rows to table '{table_name}'")
    except Exception as e:
        print(f"Error saving to DB: {e}")

def load_from_db(query):
    """
    Executes a SQL query and returns a Pandas DataFrame.
    """
    try:
        with engine.connect() as conn:
            return pd.read_sql(text(query), conn)
    except Exception as e:
        print(f"Error loading from DB: {e}")
        return pd.DataFrame()