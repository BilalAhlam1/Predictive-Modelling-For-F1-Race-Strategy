from databaseManager import engine
from sqlalchemy import text

def create_tables():
    print("🔨 Initializing Local Database Schema...")
    
    with engine.connect() as conn:
        
        # -------------------------------------------------------
        # 1. ML Training Data Table
        # -------------------------------------------------------
        # Stores lap-by-lap data merged with weather and tyre info.
        # Used for Phase 2: Predictive Modelling.
        print("   - Creating table: ml_training_data")
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

        # -------------------------------------------------------
        # 2. Race Telemetry Table
        # -------------------------------------------------------
        # Stores high-frequency (x,y,z) position data.
        # Used for Phase 1: Dashboard Replay Visualisation.
        print("   - Creating table: race_telemetry")
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
        
        # -------------------------------------------------------
        # 3. Indexes
        # -------------------------------------------------------
        print("   - Creating indexes...")
        
        # Optimizes: "Get me all training data for Max Verstappen in this race"
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ml_lookup ON ml_training_data (session_key, driver_number);"))
        
        # Optimizes: "Get me the replay positions for Lap 5" (Critical for Dashboard smoothness)
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_telemetry_lookup ON race_telemetry (session_key, lap_number);"))

        conn.commit()
        print("Database setup complete. 'f1_strategy.db' is ready.")

if __name__ == "__main__":
    create_tables()