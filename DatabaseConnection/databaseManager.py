import os
import pandas as pd
from sqlalchemy import create_engine, text

# --- CONFIGURATION ---
def get_db_path():
    """
    Returns the database path in the same directory as this file.
    Creates the directory if it does not exist.
    """
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    os.makedirs(BASE_DIR, exist_ok=True)
    return os.path.join(BASE_DIR, "f1_strategy.db")

DB_PATH = get_db_path()
DB_URL = f"sqlite:///{DB_PATH}"
engine = create_engine(DB_URL, connect_args={"timeout": 15})

# --- HELPER FUNCTIONS ---
def save_to_db(df, table_name, if_exists='append'):
    """
    Saves a Pandas DataFrame to the database.
    
    Args:
        df (pd.DataFrame): Data to save.
        table_name (str): Target table name.
        if_exists (str): 'append', 'replace', or 'fail'. Default: 'append'.
    """
    if df.empty:
        return

    try:
        with engine.begin() as conn:
            df.to_sql(table_name, conn, if_exists=if_exists, index=False)
    except Exception as e:
        raise Exception(f"Error saving to database: {e}")

def load_from_db(query):
    """
    Executes a SQL query and returns results as a Pandas DataFrame.
    
    Args:
        query (str): SQL query to execute.
        
    Returns:
        pd.DataFrame: Query results, or empty DataFrame if error occurs.
    """
    try:
        with engine.connect() as conn:
            return pd.read_sql(text(query), conn)
    except Exception as e:
        raise Exception(f"Error loading from database: {e}")

def execute_query(query, params=None):
    """
    Executes a data manipulation query (INSERT, UPDATE, DELETE).
    
    Args:
        query (str): SQL query to execute.
        params (dict, optional): Query parameters for parameterized queries.
    """
    try:
        with engine.begin() as conn:
            conn.execute(text(query), params or {})
    except Exception as e:
        raise Exception(f"Error executing query: {e}")

def test_db_connection():
    """
    Tests the database connection.
    
    Returns:
        bool: True if connection successful, False otherwise.
    """
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False