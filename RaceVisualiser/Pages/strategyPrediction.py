import sys
import streamlit as st
import os
import joblib
import numpy as np
import pandas as pd
import json
from sqlalchemy import create_engine, text
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.metrics import mean_absolute_error, mean_squared_error
from pathlib import Path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'DataCollection')))
import storeRaceData as raceData
import storeMLData as mlData

st.session_state['selected_session_key'] = 9839
st.session_state['selected_race_name'] = "Sample Grand Prix"

# ---- GLOBAL THEME FOR RACE REPLAY ----
st.markdown(
    """
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&display=swap');

        :root {
            --bg: radial-gradient(circle at 10% 20%, #0f172a 0%, #0b1020 35%, #050813 70%);
            --panel: rgba(255, 255, 255, 0.04);
            --panel-strong: rgba(255, 255, 255, 0.08);
            --text: #e5e7eb;
            --muted: #94a3b8;
            --accent: #7cf2d4;
            --accent-2: #7aa2ff;
            --pill: rgba(255,255,255,0.08);
            --border: rgba(255,255,255,0.12);
            --shadow: 0 24px 60px rgba(0,0,0,0.45);
        }

        html, body, [class^="css"], [class*="css"] {
            font-family: 'Space Grotesk', 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
        }

        body {
            background: var(--bg);
            color: var(--text);
        }

        .block-container {
            padding: 2.5rem 2.25rem 3rem 2.25rem;
            max-width: 100% !important;
            width: 100% !important;
        }
        
        /* Streamlit's default container restrictions to adjust width based on screen size */
        .main .block-container {
            max-width: 100% !important;
            padding-left: 5% !important;
            padding-right: 5% !important;
        }

        /* Hero for Race Replay */
        .hero-shell {
            background: linear-gradient(135deg, rgba(124, 242, 212, 0.12), rgba(122, 162, 255, 0.10));
            border: 1px solid var(--border);
            box-shadow: var(--shadow);
            border-radius: 24px;
            padding: 22px 26px;
            display: flex;
            align-items: center;
            gap: 18px;
            margin-bottom: 20px;
        }

        .hero-pill {
            background: var(--pill);
            color: var(--text);
            padding: 8px 14px;
            border-radius: 999px;
            font-size: 13px;
            border: 1px solid var(--border);
            letter-spacing: 0.03em;
        }

        .hero-title {
            font-size: 28px;
            font-weight: 700;
            margin: 0;
            color: #f8fafc;
        }

        .hero-subtext {
            margin: 2px 0 0 0;
            color: var(--muted);
            font-size: 14px;
        }

        /* Cards for Session Overview */
        .glass-card {
            background: var(--panel);
            border: 1px solid var(--border);
            border-radius: 18px;
            padding: 18px 18px 14px 18px;
            box-shadow: var(--shadow);
            margin-bottom: 18px;
        }

        .card-title {
            font-size: 16px;
            font-weight: 600;
            color: #e2e8f0;
            margin-bottom: 10px;
            letter-spacing: 0.01em;
        }

        .metric-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
            gap: 12px;
            margin: 12px 0 4px 0;
        }

        .metric-tile {
            background: var(--panel-strong);
            border: 1px solid var(--border);
            border-radius: 14px;
            padding: 12px 14px;
        }

        .metric-label {
            color: var(--muted);
            font-size: 13px;
            margin-bottom: 2px;
        }

        .metric-value {
            color: #f8fafc;
            font-size: 20px;
            font-weight: 600;
        }

        .element-container .stPlotlyChart {
            border-radius: 14px;
            overflow: hidden;
        }

        /* Form controls */
        .stSelectbox > div > div {
            border-radius: 12px;
            border: 1px solid var(--border);
            background: rgba(255,255,255,0.03);
        }

        .stMarkdown a { color: var(--accent); }

    </style>
    """,
    unsafe_allow_html=True,
)

# ------------------ RACE REPLAY SECTION ------------------ # 
st.markdown(
    f"""
    <div class="hero-shell">
        <div class="hero-pill">Race Replay · Telemetry</div>
        <div>
            <div class="hero-title">{st.session_state.get('selected_race_name', 'Race Replay')}</div>
            <div class="hero-subtext">Session Key: {st.session_state.get('selected_session_key', '—')}</div>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

if 'selected_session_key' not in st.session_state:
    st.warning("No race selected.")
    st.stop()

session_key = st.session_state['selected_session_key']
race_name = st.session_state.get('selected_race_name', 'Unknown GP')

# --- DATA LOADING PHASE ---
# If data hasn't been checked yet, show spinner and hide sidebar
if "replay_loaded" not in st.session_state:
    
    # Hide Sidebar, Header, and Center the Spinner
    st.markdown(
        """
        <style>
            /* Center the spinner vertically and horizontally */
            .stSpinner {
                position: fixed;
                top: 50%;
                left: 50%;
                transform: translate(-50%, -50%);
                text-align: center;
            }
        </style>
        """,
        unsafe_allow_html=True
    )

#----------------- TRACK LAYOUT ------------------#
@st.cache_data
def get_static_track(key):
    return raceData.get_track_layout(key)

# ------------------ IMPORT DATABASES AND MODELS ------------------ #
current_script_path = Path(__file__).resolve().parent

# Go up two levels to get to the Repository Root
repo_root = current_script_path.parent.parent 

# Construct the path to the database folder
db_folder = repo_root / 'DatabaseConnection'
db_file = db_folder / 'f1_strategy.db'

# specific debug check
if not db_folder.exists():
    st.error(f"Database folder not found at {db_folder}")
    st.stop()
elif not db_file.exists():
    st.error(f"Database file not found")
    st.stop()
else:
    print(f"Database found")

# Connect using the absolute path
DB_URL = f"sqlite:///{db_file}"
engine = create_engine(DB_URL)
print(f"Connected to Database")

# Load the saved assets
base_path = '../../TrainingModel/models/'

# Load assets using the base path
MODEL = joblib.load(base_path + 'lap_times_v1_model.joblib')
MODEL_COLS = joblib.load(base_path + 'lap_times_v1_columns.joblib')

with open(base_path + 'lap_times_v1_metrics.json', 'r') as f:
    GLOBAL_METRICS = json.load(f)

rf_model = MODEL

# Fetch data from ml_training_data database
session_key = session_key
def load_from_db():
    """
    Executes a SQL query and returns a Pandas DataFrame.
    """
    try:
        with engine.connect() as conn:
            return pd.read_sql(f"""SELECT * FROM ml_training_data WHERE session_key = {session_key}""", conn)
    except Exception as e:
        print(f"Error loading from DB: {e}")
        return pd.DataFrame()

data = load_from_db()
    
# ------------------ PIT LOSS MODEL ------------------ #
def get_historic_pit_loss(driver_id, session_key):
    """
    Calculates the average time lost during pit stops. 
    Returns a single float value.
    """
    
    # Query to get lap data for the specified driver and session
    query = text(f"""
        SELECT lap_number, lap_duration, is_pit_out_lap
        FROM ml_training_data 
        WHERE session_key = {session_key} 
        AND driver_number = {driver_id}
        ORDER BY lap_number
    """)
    
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    
    # Filter for pit out laps
    pit_out_laps = df[df['is_pit_out_lap'] == 1]['lap_number'].tolist()
    losses = []

    for lap in pit_out_laps:
        # Get pit lap time
        pit_time_series = df.loc[df['lap_number'] == lap, 'lap_duration']
        if pit_time_series.empty: continue
        pit_time = pit_time_series.values[0]
        
        # Compare vs clean lap (lap + 2) and (lap + 3) to avoid immediate out-lap effects
        clean_candidates = df.loc[
            (df['lap_number'].isin([lap + 2, lap + 3])) & (df['is_pit_out_lap'] == 0), 
            'lap_duration'
        ]
        
        # Calculate loss if clean candidates exist by averaging them and subtracting from pit time
        if not clean_candidates.empty:
            loss = pit_time - clean_candidates.mean()
            losses.append(loss)
    
    # Return average if data exists, else default to 20s
    if losses:
        return float(np.mean(losses))
    else:
        return 20.0
    
# ------------------ BUILD TIMELINE DATAFRAME ------------------ #
def predict_strategy_with_history(driver_id, start_lap, end_lap, pit_stop_lap, tire_compound, track_temp, air_temp, historic_loss):

    stint_predictions = []
    current_tire_age = 1 

    print(f"\nSimulating Stint: Laps {start_lap}-{end_lap} | Pitting on Lap {pit_stop_lap}")

    for lap_number in range(start_lap, end_lap + 1):
        
        # Prepare Input
        input_data = {col: 0 for col in MODEL_COLS}
        input_data['laps_on_tire'] = current_tire_age
        input_data['fuel_proxy'] = -1 * lap_number
        input_data['track_temperature'] = track_temp
        input_data['air_temperature'] = air_temp
        input_data['rainfall'] = 0
        
        if f"driver_number_{driver_id}" in input_data:
            input_data[f"driver_number_{driver_id}"] = 1
        if f"tire_compound_{tire_compound}" in input_data:
            input_data[f"tire_compound_{tire_compound}"] = 1
            
        # Predict Clean Time
        input_df = pd.DataFrame([input_data])
        input_df = input_df[MODEL_COLS]
        predicted_clean_time = rf_model.predict(input_df)[0].sum()
        
        # Apply Pit Logic
        final_time = predicted_clean_time
        note = "Clean Lap"
        
        if lap_number == pit_stop_lap:
            final_time += historic_loss
            note = f"PIT STOP (+{historic_loss:.2f}s)"
            current_tire_age = 0 # Reset tires
            
        stint_predictions.append({
            'Lap': lap_number,
            'Time': round(final_time, 3),
            'Tire Age': current_tire_age,
            'Note': note
        })
        
        current_tire_age += 1
        
    return pd.DataFrame(stint_predictions)

# ------------------ MODEL ACCURACY ------------------ #
def calculate_model_accuracy(simulation_df, session_key, driver_id):
    """
    Fetches actual lap times and compares them to the simulation.
    Returns the dataframe with actuals added, and a dictionary of metrics.
    """
    # Get the Lap Range from the simulation
    start_lap = simulation_df['Lap'].min()
    end_lap = simulation_df['Lap'].max()
    
    # Fetch Actual History
    query = text(f"""
        SELECT lap_number as Lap, lap_duration as Actual_Time
        FROM ml_training_data 
        WHERE session_key = {session_key} 
        AND driver_number = {driver_id}
        AND lap_number BETWEEN {start_lap} AND {end_lap}
    """)
    
    with engine.connect() as conn:
        actual_df = pd.read_sql(query, conn)
        
    # Merge Simulation with Actuals
    # We use 'left' merge to keep all simulation rows
    comparison_df = pd.merge(simulation_df, actual_df, on='Lap', how='left')
    
    # Calculate Errors
    # Filter out rows where Actual_Time might be missing (DNFs etc)
    valid_comparison = comparison_df.dropna(subset=['Actual_Time', 'Time'])
    
    if valid_comparison.empty:
        return comparison_df, {"MAE": 0, "RMSE": 0, "Delta": 0}
    
    mae = mean_absolute_error(valid_comparison['Actual_Time'], valid_comparison['Time'])
    rmse = np.sqrt(mean_squared_error(valid_comparison['Actual_Time'], valid_comparison['Time']))
    delta = valid_comparison['Time'].sum() - valid_comparison['Actual_Time'].sum()
    
    # Total Race Time Difference
    total_sim_time = valid_comparison['Time'].sum()
    total_actual_time = valid_comparison['Actual_Time'].sum()
    delta = total_sim_time - total_actual_time
    confidence_score = max(0, 100 - mae)
    
    metrics = {
        "MAE": mae,                             # Average error per lap (seconds)
        "RMSE": rmse,                           # Penalizes big outliers (like pit stop errors)
        "Total_Delta": delta,                   # Positive = Sim was slower, Negative = Sim was faster
        "Confidence_Score": confidence_score    # Confidence out of 100
    }
    
    return comparison_df, metrics

# ------------------ TRAFFIC MODEL ------------------ #
def build_traffic_map(session_key):
    """
    Builds the lookup table for Ghost Cars.
    """
    #print("Building Traffic Map...")
    query = text(f"""
        SELECT driver_number, lap_number, lap_duration 
        FROM ml_training_data 
        WHERE session_key = {session_key} 
        ORDER BY driver_number, lap_number
    """)
    
    with engine.connect() as conn: 
        df = pd.read_sql(query, conn)
        
    # Calculate Cumulative Race Time (Total of lap durations)
    df['race_time'] = df.groupby('driver_number')['lap_duration'].cumsum()
    return df

def build_historic_pace_map(session_key):
    """
    Pre-fetches all lap times for fast lookup during battles.
    Returns a dict: pace_map[driver_id][lap_number] = lap_time.
    Used to check for opponents' pace without querying the DB repeatedly.
    """
    query = text(f"SELECT driver_number, lap_number, lap_duration FROM ml_training_data WHERE session_key={session_key}")
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    
    # Convert to nested dictionary for O(1) lookup
    pace_map = {}
    for _, row in df.iterrows():
        d_id = int(row['driver_number'])
        lap = int(row['lap_number'])
        if d_id not in pace_map: pace_map[d_id] = {}
        pace_map[d_id][lap] = row['lap_duration']
        
    return pace_map

def check_traffic(current_lap, current_cum_time, predicted_pace, driver_id, traffic_map, ignore_list=[]):
    
    # Filter Logic
    traffic = traffic_map[
        (traffic_map['lap_number'] == current_lap) & 
        (traffic_map['driver_number'] != driver_id) &
        (~traffic_map['driver_number'].isin(ignore_list))
    ].copy()
    
    traffic['gap'] = current_cum_time - traffic['race_time']
    cars_ahead = traffic[(traffic['gap'] > 0) & (traffic['gap'] < 1.5)].sort_values('gap') # Lowered gap detection to 1.5s (dirty air zone) for tighter racing.
    
    if cars_ahead.empty:
        return predicted_pace, "Clean Air", None
    
    target = cars_ahead.iloc[0]
    target_id = int(target['driver_number'])
    target_pace = target['lap_duration'] 
    
    pace_delta = target_pace - predicted_pace
    
    # If we are 0.4s faster, we go for the overtake
    if pace_delta > 0.4: 
        return predicted_pace + 0.3, f"Overtake #{target_id}", target_id
    
    # Match their speed exactly if blocked. Do not add +0.2s.
    elif pace_delta > -0.1: 
        return target_pace, f"Blocked by #{target_id}", None
    
    else: 
        # They are faster, we chase.
        return predicted_pace, f"Chasing #{target_id}", None


# ------------------ RUN SIMULATION ------------------ #
def run_simulation(driver_id, session_key, start_lap, end_lap, pit_lap, historic_pit_lap, tire_compound, track_temp, air_temp, pace_bias=0.0, history_map=None):
    
    # intiialize
    historic_loss = get_historic_pit_loss(driver_id, session_key)
    traffic_map = build_traffic_map(session_key)
    pace_map = build_historic_pace_map(session_key)
    
    # Approximate cliff points for tire compounds
    TIRE_LIMITS = {
        'SOFT': 18,    # Performance drops after 18 laps
        'MEDIUM': 28,  # Performance drops after 28 laps
        'HARD': 45,    # Performance drops after 45 laps
        'INTERMEDIATE': 30,
        'WET': 30
    }
    
    # Get initial race time (Start of simulation window)
    query = text(f"SELECT SUM(lap_duration) FROM ml_training_data WHERE session_key={session_key} AND driver_number={driver_id} AND lap_number < {start_lap}")
    with engine.connect() as conn: 
        start_time = conn.execute(query).scalar() or 0.0
        
    current_race_time = start_time
    results = []
    
    # Model Columns Check
    if hasattr(rf_model, "feature_names_in_"): correct_cols = rf_model.feature_names_in_
    else: correct_cols = MODEL_COLS 

    #print(f"\nSTARTING SIMULATION (Bias: {pace_bias:.3f} s/lap)")

    # memory structures for battle tracking
    # passed_cars_memory: set(driver_ids) for cars we've passed recently
    active_battles = {}  # dict(driver_id: lap_number_passed)
    passed_cars_memory = set()
    results = []
    
    # ---------------- SIMULATION STAGE ------------------ #
    # We need to track tire age manually for the simulation phases
    # Start with whatever age the tire was at start_lap
    if start_lap in history_map:
        virtual_tire_age = history_map[start_lap]['tire_age']
        current_virtual_compound = history_map[start_lap]['tire_compound'] 
    else:
        virtual_tire_age = 1
        current_virtual_compound = 'HARD' # Fallback

    # ---------------- LOOP THROUGH LAPS ------------------ #
    for lap in range(start_lap, end_lap + 1):
        
        note = ""
        is_pit_lap = (lap == pit_lap)
        
        # --- HISTORIC REPLAY ---
        # If we are before the historic pit AND before our new pit, we are just replaying reality.
        should_use_history = (lap < pit_lap) and (lap < historic_pit_lap)
        
        if should_use_history and (lap in history_map):
            # Copy historic data directly
            final_time = history_map[lap]['lap_duration']
            current_virtual_compound = history_map[lap]['tire_compound']
            virtual_tire_age = history_map[lap]['tire_age']
            note = "Historic Data"
            
        else:
            # DETERMINE SIMULATION STAGE
            if is_pit_lap:
                current_virtual_compound = tire_compound # Switch to selected tire
                virtual_tire_age = 0 # Reset age
                note = f"PIT STOP (+{historic_loss}s)"
                
            elif lap > pit_lap:
                # After the new pit stop
                current_virtual_compound = tire_compound
                # Age increments naturally at end of loop
                
            else:
                # We stayed out past historic pit
                # Keep using the old compound (passed from previous lap state)
                note = "Stint Extension"

            # Prepare Input Data
            input_data = {col: 0 for col in correct_cols}
            input_data['laps_on_tire'] = virtual_tire_age
            input_data['fuel_proxy'] = -1 * lap
            input_data['track_temperature'] = track_temp
            input_data['air_temperature'] = air_temp
            #print (f"Lap {lap}: Tire Age={virtual_tire_age}, Compound={current_virtual_compound}")
            # One-Hot Encoding for Driver & Compound
            if f"driver_number_{driver_id}" in input_data: input_data[f"driver_number_{driver_id}"] = 1
            if f"tire_compound_{current_virtual_compound}" in input_data: input_data[f"tire_compound_{current_virtual_compound}"] = 1
            
            # Model For Historic Compound & Age
            raw_pace = rf_model.predict(pd.DataFrame([input_data]))[0].sum()
            base_time = raw_pace - pace_bias
            
            # --- TIRE DEGRADATION ---
            # If the tire is unrealistically old, apply exponential penalty
            deg_penalty = 0.0
            limit = TIRE_LIMITS.get(current_virtual_compound, 30)
            
            if virtual_tire_age > limit:
                excess_laps = virtual_tire_age - limit
                # Formula: 0.08 * (Excess ^ 1.6)
                deg_penalty = 0.08 * (excess_laps ** 1.6)
                if deg_penalty > 0.5:
                    note += f" [Degradation: +{deg_penalty:.1f}s]"

            # --- BATTLE LOGIC ---
            # Counter-Overtake Logic
            # Check if anyone we recently passed is fighting back and re-passing us.
            battle_penalty = 0.0
            
            # Copy keys to modify dict safely
            for opp_id in list(active_battles.keys()):
                pass_lap = active_battles[opp_id]
                
                # If 2 laps have passed, the battle is won. Move to permanent memory.
                if (lap - pass_lap) > 2:
                    del active_battles[opp_id]
                    passed_cars_memory.add(opp_id)
                    continue
                
                # Check Opponent's Pace
                opp_pace = pace_map.get(opp_id, {}).get(lap, 999.0)
                
                # Re-pass Logic
                # If Opponent is significantly faster (>0.2s) than our base time, they re-pass.
                if (base_time - opp_pace) > 0.2:
                    battle_penalty += 0.8 # We lose significant time being passed
                    note += f"Re-passed by #{opp_id}!"
                    
                    # Remove from battles AND memory. We must fight them again next traffic check.
                    del active_battles[opp_id]
                    if opp_id in passed_cars_memory: passed_cars_memory.remove(opp_id)
                else:
                    # We defend successfully
                    battle_penalty += 0.05 # Tiny time loss for defensive line
            
            # Apply penalties
            tentative_time = base_time + battle_penalty + deg_penalty
            
            # Strategy Pit Logic
            if is_pit_lap:
                final_time = tentative_time + historic_loss
                note = f"PIT STOP (+{historic_loss:.1f}s)"
                virtual_tire_age = 0 # Reset tires
            else:
                # Run Standard Traffic Check
                # Ignore passed cars memory for this check.
                # This ensures we don't crash into someone we just passed if we are still close.
                ignore_list = list(passed_cars_memory)
                
                final_time, traffic_note, passed_id = check_traffic(
                    lap, current_race_time, tentative_time, driver_id, traffic_map, ignore_list
                )
                
                if "Clean" not in traffic_note: 
                    note = f"{note} {traffic_note}".strip()
                
                # If we passed someone, start the battle timer
                if passed_id:
                    active_battles[passed_id] = lap
            
        # --- UPDATE STATE ---
        current_race_time += final_time
        virtual_tire_age += 1 # Increment age for the next lap
        
        results.append({
            "Lap": lap,
            "Time": round(final_time, 3),
            "CumTime": round(current_race_time, 2),
            "Note": note.strip()
        })

    return pd.DataFrame(results)

# ------------------ BUILD DRIVER BIAS ------------------ #
def calibrate_and_simulate(driver_id, session_key, start_lap, end_lap, pit_lap, historic_pit_lap, tire_compound, track_temp, air_temp, historic_map):
    """
    1. Runs Control Simulation to find Bias.
    2. Runs Final Simulation with Bias + Re-Overtake Logic.
    """
    
    # We look up the compound used immediately before the historic pit stop.
    post_pit_lap = historic_pit_lap - 1
    
    if post_pit_lap in historic_map:
        historic_compound = historic_map[post_pit_lap]['tire_compound']
    else:
        # Fallback if map is incomplete
        historic_compound = tire_compound
        
    # Calibration Phase
    #print("PHASE 1: Calibration Run")
    #print("Historic Next Compound:", historic_next_compound)
    control_df = run_simulation(
        driver_id, session_key, start_lap, end_lap, 
        pit_lap=historic_pit_lap, 
        historic_pit_lap=historic_pit_lap,
        tire_compound=historic_compound, track_temp=track_temp, air_temp=air_temp, 
        pace_bias=0.0,
        history_map=historic_map
    )
    
    # Calculate Bias
    query = text(f"SELECT SUM(lap_duration) FROM ml_training_data WHERE session_key={session_key} AND driver_number={driver_id} AND lap_number BETWEEN {start_lap} AND {end_lap}")
    with engine.connect() as conn: actual_total = conn.execute(query).scalar()
        
    sim_total = control_df['Time'].sum()
    bias = (sim_total - actual_total) / (end_lap - start_lap + 1)
    
    # Strategy Phase
    #print(f"PHASE 2: Final Strategy (Bias: {bias:.3f})...")
    final_df = run_simulation(
        driver_id, session_key, start_lap, end_lap, 
        pit_lap=pit_lap, 
        historic_pit_lap=historic_pit_lap,
        tire_compound=tire_compound, track_temp=track_temp, air_temp=air_temp, 
        pace_bias=bias,
        history_map=historic_map
    )
    
    return final_df, bias


# ------------------ INTERPOLATE COORDINATES ------------------ #
def fetch_driver_sector_times_and_position(session_key, driver_id):
    """
    Fetches sector times and position data for a driver in a session.
    Returns a DataFrame with lap_number, sector_1_time, sector_2_time, sector_3_time, position.
    """
    query = text(f"""
        SELECT 
            t.lap_number,
            m.duration_sector_1, 
            m.duration_sector_2, 
            m.duration_sector_3, 
            t.x, 
            t.y
        FROM race_telemetry t
        JOIN ml_training_data m 
            ON t.session_key = m.session_key 
            AND t.driver_number = m.driver_number 
            AND t.lap_number = m.lap_number
        WHERE t.session_key = {session_key} 
            AND t.driver_number = {driver_id}
            AND t.lap_number = (
                -- Subquery to find the single lap closest to the average race pace
                SELECT lap_number 
                FROM ml_training_data 
                WHERE session_key = {session_key} 
                    AND driver_number = {driver_id} 
                    AND lap_duration IS NOT NULL
                ORDER BY ABS(lap_duration - (
                    SELECT AVG(lap_duration) 
                    FROM ml_training_data 
                    WHERE session_key = {session_key} 
                        AND driver_number = {driver_id}
                )) ASC
                LIMIT 1
            )
        ORDER BY t.timestamp
    """)
    
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
        
    return df

def generate_ghost_lap(ref_df, predicted_sectors, driver_id, lap_number):
    """
    ref_df: The dataframe you fetched (x, y, duration_sector_1, etc.)
    predicted_sectors: Dict with keys 'duration_sector_1', 'duration_sector_2', 'duration_sector_3'
    """
    
    # Get Reference Sector Durations
    ref_s1 = ref_df['duration_sector_1'].iloc[0]
    ref_s2 = ref_df['duration_sector_2'].iloc[0]
    ref_s3 = ref_df['duration_sector_3'].iloc[0]
    ref_total = ref_s1 + ref_s2 + ref_s3
    
    # Calculate Split Indices based on Reference Time Ratios (for example 20%, 35%, 45%)
    n_points = len(ref_df)
    
    # Calculate approximate index where each sector ends (based on reference times)
    idx_s1 = int(n_points * (ref_s1 / ref_total))
    idx_s2 = int(n_points * ((ref_s1 + ref_s2) / ref_total))
    
    # Create the Ghost Timeline
    # We create a new race_time array that fits the predicted duration
    
    # --- Sector 1 (Indices 0 to idx_s1) ---
    # Linspace generates evenly spaced times from 0 to Predicted S1
    t_s1 = np.linspace(0, predicted_sectors['duration_sector_1'], num=idx_s1, endpoint=False)
    
    # --- Sector 2 (Indices idx_s1 to idx_s2) ---
    # Start: Pred S1
    # End: Pred S1 + Pred S2
    start_s2 = predicted_sectors['duration_sector_1']
    end_s2 = start_s2 + predicted_sectors['duration_sector_2']
    t_s2 = np.linspace(start_s2, end_s2, num=(idx_s2 - idx_s1), endpoint=False)
    
    # --- Sector 3 (Indices idx_s2 to End) ---
    # Start: Pred S1 + Pred S2
    # End: Pred Total
    start_s3 = end_s2
    end_s3 = start_s3 + predicted_sectors['duration_sector_3']
    t_s3 = np.linspace(start_s3, end_s3, num=(n_points - idx_s2))
    
    # Combine and Assign
    new_race_time = np.concatenate([t_s1, t_s2, t_s3])
    
    # Create the Ghost DataFrame
    ghost_df = ref_df[['x', 'y']].copy()
    ghost_df['race_time'] = new_race_time
    ghost_df['lap_number'] = lap_number
    ghost_df['driver_id'] = driver_id + "_PRED"
    
    return ghost_df

def fetch_pit_lap_telemetry(session_key, driver_id, lap_number):
    """
    Fetches x, y, and sector times for a PIT lap.
    """
    query = text(f"""
        SELECT 
            t.lap_number,
            m.duration_sector_1, 
            m.duration_sector_2, 
            m.duration_sector_3, 
            t.x, 
            t.y
        FROM race_telemetry t
        JOIN ml_training_data m 
            ON t.session_key = m.session_key 
            AND t.driver_number = m.driver_number 
            AND t.lap_number = m.lap_number
        WHERE t.session_key = {session_key} 
          AND t.driver_number = {driver_id}
          AND t.lap_number = {lap_number}
          AND m.is_pit_out_lap = 1
        ORDER BY t.timestamp
    """)
    
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df

def simulate_stint (session_id, driver_id, pit_lap, historic_pit_lap, tire_compound, start_lap, end_lap):
    """
    Simulates a stint for a driver in a session, returning coordinates, and timestamps, and includes battles.
    """
    
    # Fetch Environmental Conditions
    query = text(f"""
        SELECT track_temperature, air_temperature
        FROM ml_training_data 
        WHERE session_key={session_id}
    """)
    
    # Fetch Driver's Lap History for the stint
    query_history = text(f"""
        SELECT lap_number, lap_duration, tire_compound, laps_on_tire as tire_age
        FROM ml_training_data 
        WHERE session_key={session_id} 
          AND driver_number={driver_id}
          AND lap_number BETWEEN {start_lap} AND {end_lap}
    """)
    
    with engine.connect() as conn:
        history_df = pd.read_sql(query_history, conn)
        temps = pd.read_sql(query, conn)
        track_temp = temps['track_temperature'].iloc[0]
        air_temp = temps['air_temperature'].iloc[0]
    
    # Build Historic Map for Reference
    history_map = history_df.set_index('lap_number').to_dict('index')
    
    # Evaluate Accuracy
    params = {
    "driver_id": driver_id, 
    "session_key": session_id, 
    "start_lap": start_lap, 
    "end_lap": end_lap, 
    "pit_lap": pit_lap,
    "historic_pit_lap": historic_pit_lap,
    "tire_compound": tire_compound, 
    "track_temp": track_temp, 
    "air_temp": air_temp,
    "historic_map": history_map   # Pass the historic map for reference
    }
    
    # Run Simulation with Battle Logic
    sim_df, bias_used = calibrate_and_simulate(**params)
    acc_df, metrics = calculate_model_accuracy(sim_df, params['session_key'], params['driver_id'])

    print(f"Bias Applied: {bias_used:.3f} s/lap")
    print(f"Total Delta: {metrics['Total_Delta']:.3f} s")
    print(f"MAE: {metrics['MAE']:.4f} %")
    print(f"RMSE: {metrics['RMSE']:.4f} %")
    print(f"Confidence Score: {metrics['Confidence_Score']:.2f}/100")
    
    
    # Fetch Two Reference Maps
    # The Fast Map (Average Racing Lap)
    ref_racing = fetch_driver_sector_times_and_position(session_id, driver_id)
    
    # The Pit Map (The Historic Pit Lap)
    # We fetch the specific lap where they actually pitted historically
    ref_pitting = fetch_pit_lap_telemetry(session_id, driver_id, historic_pit_lap)
    
    if ref_racing.empty or ref_pitting.empty or sim_df.empty:
        print("Error: Missing telemetry data for racing or pit reference.")
        return pd.DataFrame()

    ghost_laps_list = []
    
    for _, row in sim_df.iterrows():
        lap_num = int(row['Lap'])
        pred_total_time = row['Time']
        
        # --- MAP SELECTION LOGIC ---
        if lap_num == pit_lap:
            # SWITCH TO PIT MAP
            current_ref = ref_pitting
            print(f"Swapping to Pit Lane Geometry for Lap {lap_num}")
        else:
            # USE RACING MAP
            current_ref = ref_racing
            
        # Get Reference Sectors for the CHOSEN map
        ref_s1 = current_ref['duration_sector_1'].iloc[0]
        ref_s2 = current_ref['duration_sector_2'].iloc[0]
        ref_s3 = current_ref['duration_sector_3'].iloc[0]
        ref_total = ref_s1 + ref_s2 + ref_s3
        
        # Distribute Predicted Time based on the CHOSEN map's ratios
        # (This automatically handles the slow S3 of the pit lap!)
        predicted_sectors = {
            'duration_sector_1': pred_total_time * (ref_s1 / ref_total),
            'duration_sector_2': pred_total_time * (ref_s2 / ref_total),
            'duration_sector_3': pred_total_time * (ref_s3 / ref_total)
        }
        
        # Generate Coordinates using the CHOSEN map
        ghost_lap = generate_ghost_lap(current_ref, predicted_sectors, driver_id=str(driver_id), lap_number=lap_num)
        
        # Sync Time
        lap_start_time = row['CumTime'] - pred_total_time
        ghost_lap['race_time'] = ghost_lap['race_time'] + lap_start_time
        ghost_lap['lap_number'] = lap_num
        
        ghost_laps_list.append(ghost_lap)
        
    # Finalize
    final_ghost = pd.concat(ghost_laps_list, ignore_index=True)
    final_ghost['driver_acronym'] = 'GHOST'
    final_ghost['team_colour'] = "#757576" 
    
    return final_ghost

# ------------------ USER INTERFACE ------------------ #
#-----------------REPLAY DATA------------------#
@st.cache_data
def get_race_data(key):
    """
    Fetches and processes race replay data for visualization.
    """
    # get_race_replay_data now returns (resampled_telemetry_df, lap_times_df)
    resampled, lap_times = raceData.get_race_replay_data(key)
    if resampled is None or (hasattr(resampled, 'empty') and resampled.empty):
        return pd.DataFrame(), pd.DataFrame()
    
    #-----------------STINT DATA FOR COMPOUND------------------#
    pit_data = mlData.fetchMLData(key)
    stints = [] # List to hold stint information: driver, start lap, end lap, compound
    if not pit_data.empty:
        # Map driver numbers to acronyms
        if 'driver_number' in resampled.columns and 'driver_acronym' in resampled.columns:
            driver_map = resampled[['driver_number', 'driver_acronym']].drop_duplicates()
            pit_data['driver_number'] = pit_data['driver_number'].astype(driver_map['driver_number'].dtype)
            pit_data = pit_data.merge(driver_map, on='driver_number', how='left')
        else:
            pit_data['driver_acronym'] = pit_data['driver_number'].astype(str) # Fallback if mapping unavailable

        y_order = pit_data['driver_acronym'].dropna().unique().tolist() # Maintain order from pit_data
        lap_col = 'lap_number'
        compound_col = 'tire_compound'
        laps_on_tire_col = 'laps_on_tire'

        # Process each driver's pit stops to determine stints
        for drv in y_order:
            ddf = pit_data[pit_data['driver_acronym'] == drv].copy().sort_values(lap_col)
            if lap_col not in ddf.columns or compound_col not in ddf.columns:
                continue
            
            # Initialize stint tracking variables
            current_comp = None
            current_start = None
            prev_lap = None
            prev_tire_age = None

            # Iterate through driver's pit data to identify stints
            #Sample row: lap_number, tire_compound, laps_on_tire
            for _, r in ddf.iterrows():
                lap = int(r[lap_col])
                comp = str(r[compound_col]).upper() if pd.notna(r[compound_col]) else 'UNKNOWN'
                current_tire_age = int(r[laps_on_tire_col]) if laps_on_tire_col in ddf.columns and pd.notna(r[laps_on_tire_col]) else (999 if prev_tire_age is None else prev_tire_age + 1)

                if current_comp is None: # First stint initialization
                    current_comp = comp
                    current_start = lap
                # Check for stint change conditions: new compound, tire age reset, or lap discontinuity (pit stop)
                elif (comp != current_comp) or (prev_tire_age is not None and current_tire_age < prev_tire_age) or (prev_lap is not None and lap > prev_lap + 1):
                    stints.append({'driver': drv, 'start': current_start, 'end': prev_lap, 'compound': current_comp})
                    current_comp = comp
                    current_start = lap
                
                # Update previous lap and tire age for next iteration
                prev_lap = lap
                prev_tire_age = current_tire_age
            
            # Append the final stint after loop
            if current_comp is not None:
                stints.append({'driver': drv, 'start': current_start, 'end': prev_lap, 'compound': current_comp})

    stint_lap_data = [] # Expanded list of laps with compounds
    # Expand stints into per-lap entries so we can merge easily
    for s in stints:
        if s['start'] is not None and s['end'] is not None:
            for lap in range(s['start'], s['end'] + 1):
                stint_lap_data.append({'driver_acronym': s['driver'], 'lap_number': lap, 'compound': s['compound']})
    
    stints_df = pd.DataFrame(stint_lap_data)

    #-----------------DRIVER COLORS------------------#
    df = resampled
    if not stints_df.empty:
        # Merge stint compounds into main dataframe for display
        df = pd.merge(df, stints_df, on=['driver_acronym', 'lap_number'], how='left')
        df['compound'] = df['compound'].fillna('Unknown')
    else:
        df['compound'] = 'Unknown'

    df_colors = raceData.get_driver_colors(key)
    if not df_colors.empty:
        df = pd.merge(df, df_colors, on='driver_acronym', how='left')
        # Fill any individual drivers that missed a color mapping
        df['team_colour'] = df['team_colour'].fillna('#FF1508')
        
        # Merge colours into lap times as well so the line graph can use them
        lap_times = pd.merge(lap_times, df_colors[['driver_acronym','team_colour']], on='driver_acronym', how='left')
        lap_times['team_colour'] = lap_times['team_colour'].fillna('#FF1508')
    else:
        # Fallback if API completely failed
        df['team_colour'] = '#FF1508'
        lap_times['team_colour'] = '#FF1508'
    # Format lap times as mm:ss.mmm for display
    def _fmt_time_seconds(val):
        try:
            t = float(val)
        except Exception:
            return ''
        mins = int(t // 60)
        secs = int(t % 60)
        millis = int(round((t - int(t)) * 1000))
        return f"{mins}:{secs:02d}.{millis:03d}"

    if not lap_times.empty:
        lap_times['lap_time_fmt'] = lap_times['lap_time'].apply(_fmt_time_seconds)
    else:
        lap_times['lap_time_fmt'] = []
        
        
    #-----------------TIME SETUP------------------#
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    start_time = df['timestamp'].min()
    df['race_time'] = (df['timestamp'] - start_time).dt.total_seconds()
    df = df.sort_values('race_time')

    # Determine Lap Start Times
    lap_start_times = df.groupby(['driver_acronym', 'lap_number'])['race_time'].min().reset_index()
    lap_start_times.rename(columns={'race_time': 'lap_start_time'}, inplace=True)
    df = pd.merge(df, lap_start_times, on=['driver_acronym', 'lap_number'], how='left')

    # Create Master Timeline to synchronize all drivers (fixes inconsistent leaderboard issues)
    # Changed from 0.2 to 1.0 to synchronsie to 1 second intervals for performance
    min_t = df['race_time'].min() # Starting from first timestamp
    max_t = df['race_time'].max() # Ending at last timestamp
    master_timeline = np.arange(min_t, max_t, 1.0) 
    
    aligned_dfs = []
    
    # Align each driver's data to the 'master' timeline which has consistent 1.0s intervals for better animation
    for driver in df['driver_acronym'].unique():
        d_data = df[df['driver_acronym'] == driver].set_index('race_time')
        d_data = d_data[~d_data.index.duplicated(keep='first')]
        
        # Expand index to include master timeline and interpolate
        union_index = d_data.index.union(master_timeline)
        d_interp = d_data.reindex(union_index)
        
        # Carry over Driver Color
        d_color = d_data['team_colour'].iloc[0] if 'team_colour' in d_data.columns else '#FF1508'
        
        # Interpolate Coords Linearly
        d_interp['x'] = d_interp['x'].interpolate(method='slinear', limit_direction='both')
        d_interp['y'] = d_interp['y'].interpolate(method='slinear', limit_direction='both')
        
        # Fill Metadata
        d_interp = d_interp.ffill().bfill()
        
        # Filter to Master Timeline
        d_interp = d_interp.reindex(master_timeline)
        d_interp['driver_acronym'] = driver
        d_interp['team_colour'] = d_color
        
        aligned_dfs.append(d_interp.reset_index().rename(columns={'index': 'race_time'})) # Reset index for concatenation
        
    unified_df = pd.concat(aligned_dfs)
    unified_df['lap_number'] = unified_df['lap_number'].fillna(0).astype(int) # Fill missing laps as 0
    return unified_df, lap_times

def start_simulation(session_key):
    with st.spinner(f"Optimizing {race_name} Data"):
        df, lap_times_df = get_race_data(session_key)
        track_df = get_static_track(session_key)

        if df.empty or track_df is None:
            st.error("Data unavailable.")
            return
        try:
            if (not lap_times_df.empty) and (lap_times_df['team_colour'].nunique() == 1) and (lap_times_df['team_colour'].iloc[0] == '#FF1508'):
                st.warning("Driver colours unavailable (using defaults). OpenF1 API may be down; using DB fallback.") # API limitation issue, will fallback to default colors
        except Exception:
            # if lap_times_df doesn't have team_colour, skip warning
            pass

        # ----------------- Top Row -----------------
        st.markdown("<div class='card-title' style='margin:14px 0 4px;'>Performance & Strategy</div>", unsafe_allow_html=True)
        left_col, right_col = st.columns([0.5, 0.5]) # Split for lap time graph and pit info (pit info to be added later)

        # --------------- DRIVER, PIT LAP, TYRE COMPOUND SELECTION (LEFT SIDE) ---------------
        with left_col:
            st.markdown("<div style='text-align:center; font-size:16px; font-weight:600; margin-bottom:4px;'>Driver Selection</div>", unsafe_allow_html=True)
            
            # returns driver ID based on acronym
            selected_driver = st.selectbox("Select Driver for Strategy Simulation", options=sorted(df['driver_acronym'].unique()), index=0)
            driver_id_query = text(f"SELECT DISTINCT driver_number FROM race_telemetry WHERE session_key={session_key} AND driver_acronym='{selected_driver}'")
            with engine.connect() as conn:
                driver_id = conn.execute(driver_id_query).scalar() or None
                
            # Fetch Historic Pit Lap for Driver for Default Value
            historic_pit_lap_query = text(f"""
                SELECT lap_number 
                FROM ml_training_data 
                WHERE session_key={session_key} 
                  AND driver_number={driver_id} 
                  AND is_pit_out_lap=1
                ORDER BY lap_number DESC
                LIMIT 1
            """)
            with engine.connect() as conn:
                historic_pit_lap = conn.execute(historic_pit_lap_query).scalar() or 40
            
            # Fetch max lap number for range limit
            max_lap_query = text(f"""
                SELECT MAX(lap_number) 
                FROM ml_training_data 
                WHERE session_key={session_key} 
                  AND driver_number={driver_id}
            """)
            with engine.connect() as conn:
                max_lap_number = conn.execute(max_lap_query).scalar()
                
            # Conditional max boundary for pit lap input
            max_pit_lap = max_lap_number - 1 if historic_pit_lap + 10 > max_lap_number else historic_pit_lap + 10
            min_pit_lap = 1 if historic_pit_lap - 10 < 1 else historic_pit_lap - 10
            
            # Selectbox for pit lap
            selected_pit_lap = st.number_input("Select Alternate Pit Stop Lap", min_value=min_pit_lap, max_value=max_pit_lap, value=historic_pit_lap, step=1)
        
        
            # Find the earliest point of interest (Historic vs New Selection)
            earliest_event_lap = min(historic_pit_lap, selected_pit_lap)
            # We use max to prevent negative lap numbers
            start_lap = max(1, earliest_event_lap - 1)
            
            # Fetch Historic Tyre Compound for Info
            historic_compound_query = text(f"""
                SELECT tire_compound 
                FROM ml_training_data 
                WHERE session_key={session_key} 
                  AND driver_number={driver_id} 
                ORDER BY lap_number DESC
                LIMIT 1
            """)
            with engine.connect() as conn:
                historic_tire_compound = conn.execute(historic_compound_query).scalar() or "UNKNOWN"
            
            # Selectbox for tire compound with default to historic
            compound_options = ['SOFT', 'MEDIUM', 'HARD']
            selected_tire_compound = st.selectbox("Select Tire Compound", options=compound_options, index=compound_options.index(historic_tire_compound))
            
            # Button to Run Simulation
            if st.button("Run Strategy Simulation"):  
                print(f"Selected Driver: {selected_driver} (ID: {driver_id}) | Pit Lap: {selected_pit_lap} | Historic Pit Lap: {historic_pit_lap} | Compound: {selected_tire_compound} | Historic Compound: {historic_tire_compound}")
                st.session_state['button_clicked'] = True
            else:
                st.session_state['button_clicked'] = False
        # --------------- RUN SIMULATION ON BUTTON CLICK ---------------
        if st.session_state.get('button_clicked', True):
            with st.spinner("Running Strategy Simulation"):

                # ------------------ MODEL STINT ------------------ #
                ghost_lap_data = simulate_stint(
                    session_id=session_key,
                    driver_id=driver_id,
                    pit_lap=selected_pit_lap,
                    historic_pit_lap=historic_pit_lap, # Using last pit lap as historic,
                    tire_compound=selected_tire_compound,
                    start_lap=start_lap,
                    end_lap=max_lap_number
                )
                if ghost_lap_data.empty:
                    st.error("Simulation failed due to missing telemetry data.")
                    return
                print (f"Generated Ghost Lap Data: {ghost_lap_data.shape[0]} rows")
                print (ghost_lap_data.head(3))
                
                # --------------- LAP TIME COMPARISON (RIGHT SIDE) ---------------
                with right_col:
                    st.markdown("<div style='text-align:center; font-size:16px; font-weight:600; margin-bottom:4px;'>Lap Time Comparison</div>", unsafe_allow_html=True)

                    # Prepare Data for Comparison Graph
                    historic_lap_times = lap_times_df[lap_times_df['driver_acronym'] == selected_driver].copy()
                    
                    # Fetch laps from start_lap to max_lap_number
                    historic_lap_times = historic_lap_times[(historic_lap_times['lap_number'] >= start_lap - 10) & (historic_lap_times['lap_number'] <= max_lap_number)]
                    
                    # Calculate Predicted Lap Times from Ghost Data by differencing race_time
                    predicted_lap_times = []
                    for lap in range(start_lap, max_lap_number + 1):
                        lap_data = ghost_lap_data[ghost_lap_data['lap_number'] == lap]
                        if not lap_data.empty:
                            lap_time = lap_data['race_time'].max() - lap_data['race_time'].min()
                            predicted_lap_times.append({'lap_number': lap, 'predicted_lap_time': lap_time})
                    predicted_lap_times_df = pd.DataFrame(predicted_lap_times)
                    
                    tire_before_pit_query = text(f"""
                        SELECT tire_compound 
                        FROM ml_training_data 
                        WHERE session_key={session_key} 
                          AND driver_number={driver_id} 
                          AND lap_number = {historic_pit_lap - 1}
                    """)
                    with engine.connect() as conn:
                        history_previous_tire_compound = conn.execute(tire_before_pit_query).scalar() or "UNKNOWN"
                    
                    # Plot line chart for historic vs predicted lap times
                    lap_fig = go.Figure()
                    
                    # Historic Lap Times
                    hist_label = f"Active Compound: {historic_tire_compound}<br>Compound before pit: {history_previous_tire_compound}"
                    hist_customdata = [hist_label] * len(historic_lap_times)
                    
                    lap_fig.add_trace(go.Scatter(
                        x=historic_lap_times['lap_number'], 
                        y=historic_lap_times['lap_time'],
                        mode='lines+markers',
                        name='Historic Lap Time',
                        line=dict(color='blue'),
                        marker=dict(size=6),
                        customdata=hist_customdata,
                        hovertemplate='Lap %{x}<br>Time: %{y:.3f}s<br>%{customdata}<extra></extra>',
                        hoverlabel=dict(
                            font_color="blue",
                            bgcolor="black"
                        )
                    ))
                    
                    # Predicted Lap Times
                    pred_label = f"Active Compound: {selected_tire_compound}<br>Compound before pit: {history_previous_tire_compound}"
                    pred_customdata = [pred_label] * len(predicted_lap_times_df)
                    
                    lap_fig.add_trace(go.Scatter(
                        x=predicted_lap_times_df['lap_number'], 
                        y=predicted_lap_times_df['predicted_lap_time'],
                        mode='lines+markers',
                        name='Predicted Lap Time',
                        line=dict(color='orange'),
                        marker=dict(size=6),
                        customdata=pred_customdata,
                        hovertemplate='Lap %{x}<br>Time: %{y:.3f}s<br>%{customdata}<extra></extra>',
                        hoverlabel=dict(
                            font_color="orange",
                            bgcolor="black"
                        )
                    ))
                    lap_fig.update_layout(
                        title=f"Lap Time Comparison for {selected_driver}",
                        xaxis_title="Lap Number",
                        yaxis_title="Lap Time (seconds)",
                        legend_title="Legend",
                        template="plotly_white",
                        height=400
                    )
                    st.plotly_chart(lap_fig, width='stretch')
                  
                  
# Start the Replay
start_simulation(session_key)