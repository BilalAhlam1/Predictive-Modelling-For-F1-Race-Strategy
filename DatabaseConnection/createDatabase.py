from databaseManager import engine
from sqlalchemy import text

def create_tables():
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS ml_training_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                
                -- Identifiers
                meeting_key INTEGER,
                session_key INTEGER,
                driver_number INTEGER,
                lap_number INTEGER,
                
                -- Timing
                date_start TIMESTAMP,
                lap_duration FLOAT,
                duration_sector_1 FLOAT,
                duration_sector_2 FLOAT,
                duration_sector_3 FLOAT,
                
                -- Speed & Segments
                st_speed FLOAT,
                i1_speed FLOAT,
                i2_speed FLOAT,
                segments_sector_1 TEXT, -- Storing list as string "[2049, 2049...]"
                segments_sector_2 TEXT,
                segments_sector_3 TEXT,
                
                -- Strategy Flags
                is_pit_out_lap BOOLEAN,
                tire_compound TEXT,
                laps_on_tire INTEGER,
                
                -- Weather Conditions
                rainfall FLOAT,
                track_temperature FLOAT,
                air_temperature FLOAT,
                humidity FLOAT,
                
                -- Constraint: Prevent duplicate rows for the same driver lap in the same session
                UNIQUE(session_key, driver_number, lap_number)
            );
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS race_telemetry (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                
                -- Identifiers
                session_key INTEGER,
                driver_acronym TEXT,
                driver_number INTEGER,
                lap_number INTEGER,
                
                -- Telemetry Data
                lap_duration FLOAT,
                timestamp TIMESTAMP,
                x INTEGER,
                y INTEGER,
                z INTEGER
            );
        """))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ml_lookup ON ml_training_data (session_key, driver_number);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_telemetry_lookup ON race_telemetry (session_key, lap_number);"))
        conn.commit()
        print("Database setup complete. 'f1_strategy.db' is ready.")

if __name__ == "__main__":
    create_tables()