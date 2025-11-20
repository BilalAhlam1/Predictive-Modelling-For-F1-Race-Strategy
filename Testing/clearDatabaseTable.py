import sys
import os
from sqlalchemy import text

# --- Setup Imports ---
# Add the path to your databaseManager
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'DatabaseConnection')))
import databaseManager as db

# --- CONFIGURATION ---
# CHANGE THIS to the session you want to delete
SESSION_TO_DELETE = 9939 

def clear_session_data(session_key):
    print(f"Attempting to delete data for Session {session_key}")
    
    tables_to_clean = ['ml_training_data', 'race_telemetry']
    
    try:
        # Open a connection and begin a transaction
        with db.engine.connect() as conn:
            for table in tables_to_clean:
                # Construct the delete query
                query = text(f"DELETE FROM {table} WHERE session_key = :key")
                
                # Execute
                result = conn.execute(query, {"key": session_key})
                
                # result.rowcount tells us how many rows were removed
                print(f"Table '{table}': Deleted {result.rowcount} rows.")
            
            # Commit the changes to the file
            conn.commit()
            print("Deletion complete. Database committed.")
            
    except Exception as e:
        print(f"Error deleting data: {e}")

if __name__ == "__main__":
    # Check if DB exists first
    if db.test_db_connection():
        clear_session_data(SESSION_TO_DELETE)
        
        # Verification check
        print("\nVerification")
        df = db.load_from_db(f"SELECT count(*) as count FROM ml_training_data WHERE session_key = {SESSION_TO_DELETE}")
        count = df.iloc[0]['count'] if not df.empty else 0
        if count == 0:
            print(f"Session {SESSION_TO_DELETE} is successfully gone.")
        else:
            print(f"Warning: {count} rows still remain.")