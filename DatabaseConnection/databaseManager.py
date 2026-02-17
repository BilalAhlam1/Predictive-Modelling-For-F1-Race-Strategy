import os
import pandas as pd
from sqlalchemy import create_engine, text

# --- CONFIGURATION ---
# Detect environment and set database path accordingly
def get_db_path():
    """
    Returns the appropriate database path based on environment.
    - Streamlit Cloud: /home/appuser/ (persistent storage)
    - Local: Same directory as this file
    """
    # Check if running on Streamlit Cloud
    if os.getenv('STREAMLIT_SERVER_HEADLESS') == 'true':
        # Streamlit Cloud persistent directory
        BASE_DIR = '/home/appuser'
        print("Running on Streamlit Cloud - using /home/appuser/")
    else:
        # Local development
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        print(f"Running locally - using {BASE_DIR}/")
    
    os.makedirs(BASE_DIR, exist_ok=True)
    return os.path.join(BASE_DIR, "f1_strategy.db")

DB_PATH = get_db_path()
DB_URL = f"sqlite:///{DB_PATH}"

print(f"Database: {DB_PATH}")

engine = create_engine(DB_URL, connect_args={"timeout": 15})

# --- HELPER FUNCTIONS ---
def save_to_db(df, table_name, if_exists='append'):
    """Saves a Pandas DataFrame to the database."""
    if df.empty:
        print(f"No data to save for {table_name}")
        return

    try:
        with engine.begin() as conn:
            df.to_sql(table_name, conn, if_exists=if_exists, index=False)
            print(f"Saved {len(df)} rows to table '{table_name}'")
    except Exception as e:
        print(f"Error saving to DB: {e}")

def load_from_db(query):
    """Executes a SQL query and returns a Pandas DataFrame."""
    try:
        with engine.connect() as conn:
            return pd.read_sql(text(query), conn)
    except Exception as e:
        print(f"Error loading from DB: {e}")
        return pd.DataFrame()
    
def execute_query(query, params=None):
    """Executes a query that changes data (INSERT, UPDATE, DELETE)."""
    try:
        with engine.begin() as conn:
            conn.execute(text(query), params)
        print(f"Query executed successfully")
    except Exception as e:
        print(f"Error executing query: {e}")

def test_db_connection():
    """Tests the database connection."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print(f"Database connection successful")
        print(f"   Location: {DB_PATH}")
        return True
    except Exception as e:
        print(f"Database connection failed: {e}")
        return False