import sys
import streamlit as st
import os
import joblib
import numpy as np
import pandas as pd
import json
from scipy.interpolate import Akima1DInterpolator
from sqlalchemy import create_engine, text
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from pathlib import Path

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'DataCollection')))
import storeRaceData as raceData
import storeMLData as mlData

# --- THEME & STYLING ---
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
        
        .main .block-container {
            max-width: 100% !important;
            padding-left: 5% !important;
            padding-right: 5% !important;
        }

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

        .element-container .stPlotlyChart {
            border-radius: 14px;
            overflow: hidden;
        }

        .stSelectbox > div > div {
            border-radius: 12px;
            border: 1px solid var(--border);
            background: rgba(255,255,255,0.03);
        }
    </style>
    """,
    unsafe_allow_html=True,
)

# --- PAGE SETUP ---
hero_placeholder = st.empty()

def render_hero_card(accuracy="—"):
    """Updates the hero card with current session info and model accuracy."""
    hero_placeholder.markdown(
        f"""
        <div class="hero-shell">
            <div class="hero-pill">Strategy Prediction</div>
            <div>
                <div class="hero-title">{st.session_state.get('selected_race_name', 'Strategy Simulation')}</div>
                <div class="hero-subtext">Session Key: {st.session_state.get('selected_session_key', '—')}</div>
                <div class="hero-subtext">Model Accuracy: {accuracy}%</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

render_hero_card(st.session_state.get('model_accuracy', 'N/A'))

if 'selected_session_key' not in st.session_state:
    st.warning("No race selected.")
    st.stop()

session_key = st.session_state['selected_session_key']
race_name = st.session_state.get('selected_race_name', 'Unknown GP')

def show_disclaimer():
    """Displays and manages the project disclaimer."""
    if "disclaimer_accepted" not in st.session_state:
        st.session_state.disclaimer_accepted = False

    if not st.session_state.disclaimer_accepted:
        with st.container(border=True):
            st.subheader("⚠️ Project Disclaimer & Ethical Disclosure")
            st.markdown(
                """
                This prediction model is for **academic and analytical purposes only**. 
                It utilizes historical data to simulate outcomes and has **no relation to real-life 
                official race results** or the actual performance of Formula 1 teams.
                
                - Predictions are stochastic and based on machine learning.
                - This tool is not affiliated with the FIA or Formula 1 entities.
                - Data sourced via OpenF1 and FastF1 APIs.
                """
            )
            if st.button("I Understand and Accept"):
                st.session_state.disclaimer_accepted = True
                st.rerun()
        st.stop()

show_disclaimer()

# --- DATABASE & MODEL SETUP ---
current_script_path = Path(__file__).resolve().parent
repo_root = current_script_path.parent.parent

db_folder = repo_root / 'DatabaseConnection'
db_file = db_folder / 'f1_strategy.db'

if not db_file.exists():
    st.error(f"Database not found. Expected path: {db_file}")
    st.stop()

DB_URL = f"sqlite:///{db_file}"
engine = create_engine(DB_URL)

# Load trained model and metadata
model_dir = repo_root / 'TrainingModel' / 'models'
try:
    MODEL = joblib.load(model_dir / 'lap_times_v1_model.joblib')
    MODEL_COLS = joblib.load(model_dir / 'lap_times_v1_columns.joblib')
    MODEL_SCALER = joblib.load(model_dir / 'lap_times_v1_scaler.joblib')
    
    with open(model_dir / 'lap_times_v1_metrics.json', 'r') as f:
        GLOBAL_METRICS = json.load(f)

    rf_model = MODEL
except FileNotFoundError as e:
    st.error(f"Model files not found at {model_dir}")
    st.stop()

#----------------- TRACK LAYOUT ------------------#
@st.cache_data
def get_static_track(key):
    return raceData.get_track_layout(key)

# --- PIT LOSS CALCULATION ---
def get_historic_pit_loss(driver_id, session_key):
    """
    Calculates the average time lost during pit stops from historical session data.
    
    Pit loss is calculated as: (In-Lap Delta) + (Out-Lap Delta) where Delta is relative to 
    reference pace. Considers only laps between 16-32 seconds to filter out VSC, SC, and penalties.
    
    Args:
        driver_id (int): Driver number.
        session_key (int): Session identifier.
        
    Returns:
        float: Average pit loss in seconds. Defaults to 22.0s if no valid data found.
    """
    query = text(f"""
        SELECT driver_number, lap_number, lap_duration, is_pit_out_lap
        FROM ml_training_data 
        WHERE session_key = {session_key}
        ORDER BY driver_number, lap_number
    """)
    
    with engine.connect() as conn:
        session_df = pd.read_sql(query, conn)
    
    if session_df.empty:
        return 22.0 # Default average pit loss if no data is available

    def calculate_losses_from_df(df_subset):
        """Helper to calculate pit losses from a driver subset."""
        valid_losses = []
        pit_out_laps = df_subset[df_subset['is_pit_out_lap'] == 1]['lap_number'].unique()
        
        for out_lap_idx in pit_out_laps:
            in_lap_idx = out_lap_idx - 1 # Assuming in-lap is immediately before out-lap
            
            # Validate lap indices and existence of lap times
            try:
                in_rows = df_subset[df_subset['lap_number'] == in_lap_idx]
                out_rows = df_subset[df_subset['lap_number'] == out_lap_idx]
                
                if in_rows.empty or out_rows.empty:
                    continue
                
                in_lap_time = in_rows['lap_duration'].values[0]
                out_lap_time = out_rows['lap_duration'].values[0]
            except Exception:
                continue
            
            # Get reference pace from 3 laps before pit stop
            ref_laps = [in_lap_idx - 1, in_lap_idx - 2, in_lap_idx - 3]
            clean_candidates = df_subset[
                (df_subset['lap_number'].isin(ref_laps)) & 
                (df_subset['is_pit_out_lap'] == 0)
            ]
            
            if clean_candidates.empty:
                continue
                
            ref_pace = clean_candidates['lap_duration'].mean()
            loss = (in_lap_time - ref_pace) + (out_lap_time - ref_pace)
            
            # Filter out unrealistic losses (e.g., VSC, SC, penalties)
            if 16.0 < loss < 32.0:
                valid_losses.append(loss)
                
        return valid_losses

    # Calculate for specific driver
    driver_data = session_df[session_df['driver_number'] == driver_id].copy()
    driver_losses = calculate_losses_from_df(driver_data)
    
    if driver_losses:
        return float(np.mean(driver_losses))
    
    # Fallback to session average
    all_losses = []
    for d_id in session_df['driver_number'].unique():
        d_subset = session_df[session_df['driver_number'] == d_id].copy()
        all_losses.extend(calculate_losses_from_df(d_subset))
        
    if all_losses:
        return float(np.mean(all_losses))
        
    return 22.0 # Final fallback if no valid losses found in entire session

# --- OVERTAKE THRESHOLD CALCULATION ---
def get_overtake_thresholds(driver_number):
    """
    Calculates DRS and non-DRS overtake thresholds based on historical overtaking behavior.
    
    Returns tuned deltas specific to the driver's aggression profile. More aggressive drivers
    require smaller pace advantages to successfully pass.
    
    Args:
        driver_number (int): Driver ID.
        
    Returns:
        tuple: (drs_delta, normal_delta) in seconds.
    """
    query = text(f"""
        SELECT driver_number, lap_number, lap_duration 
        FROM ml_training_data 
        WHERE session_key = {session_key}
        ORDER BY driver_number, lap_number
    """)
    
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    if df.empty:
        return 1.5, 0.6 # Default deltas if no data is available
    
    # Sort and prepare data for position calculation
    df = df.sort_values(['driver_number', 'lap_number'])
    df['race_time'] = df.groupby(['driver_number'])['lap_duration'].cumsum()
    df['position'] = df.groupby(['lap_number'])['race_time'].rank(method='first')
    df['field_avg'] = df.groupby(['lap_number'])['lap_duration'].transform('mean')

    driver_df = df[df['driver_number'] == driver_number].copy()
    
    if driver_df.empty:
        return 1.5, 0.6 # Default deltas if driver has no data

    # Identify overtakes by comparing position changes lap-over-lap, filtering out first lap and outliers
    driver_df['prev_position'] = driver_df['position'].shift(1)
    overtakes = driver_df[
        (driver_df['position'] < driver_df['prev_position']) & 
        (driver_df['lap_number'] > 1) &
        (driver_df['lap_duration'] < 120)
    ].copy()

    aggression_score = 0.5 # Default to neutral aggression if no overtakes
    
    # Calculate average pace advantage during overtakes to determine aggression profile by comparing lap duration to field average
    if not overtakes.empty:
        overtakes['pace_advantage'] = overtakes['field_avg'] - overtakes['lap_duration']
        avg_pass_delta = overtakes['pace_advantage'].mean()
        clamped_delta = np.clip(avg_pass_delta, 0.2, 1.5)
        aggression_score = 1.0 - ((clamped_delta - 0.2) / 1.3)

    # Tune overtake thresholds based on aggression score. More aggressive drivers can succeed with smaller pace advantages.
    AVG_DRS_DELTA = 1.0
    AVG_NORMAL_DELTA = 0.6
    modifier = 1.1 - (aggression_score * 0.4)
    
    return round(AVG_DRS_DELTA * modifier, 3), round(AVG_NORMAL_DELTA * modifier, 3)

# --- TRAFFIC MAP & PACE MAP ---
def build_traffic_map(session_key):
    """Builds cumulative race time lookup table for traffic detection."""
    query = text(f"""
        SELECT driver_number, lap_number, lap_duration 
        FROM ml_training_data 
        WHERE session_key = {session_key}
        ORDER BY driver_number, lap_number
    """)
    
    with engine.connect() as conn: 
        df = pd.read_sql(query, conn)
    
    # Group by driver and calculate cumulative race time to determine relative positions on track at any given lap    
    df['race_time'] = df.groupby('driver_number')['lap_duration'].cumsum()
    return df

def build_historic_pace_map(session_key):
    """Pre-fetches all lap times for O(1) lookup during traffic checks."""
    query = text(f"SELECT driver_number, lap_number, lap_duration FROM ml_training_data WHERE session_key={session_key}")
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    
    # Create a nested dictionary: pace_map[driver_id][lap_number] = lap_duration for quick access during simulation
    pace_map = {}
    for _, row in df.iterrows():
        d_id = int(row['driver_number'])
        lap = int(row['lap_number'])
        if d_id not in pace_map:
            pace_map[d_id] = {}
        pace_map[d_id][lap] = row['lap_duration']
        
    return pace_map

def check_traffic(current_lap, current_finish_time, predicted_pace, driver_id, traffic_map, ignore_list=[]):
    """
    Determines traffic impact on predicted lap time and identifies overtaking opportunities.
    
    Implements DRS train detection and battle logic to update pace predictions when traffic is present.
    
    Args:
        current_lap (int): Current lap number.
        current_finish_time (float): Cumulative race time at lap start.
        predicted_pace (float): Predicted clean air pace.
        driver_id (int): Driver number.
        traffic_map (pd.DataFrame): Cumulative race time data.
        ignore_list (list): Driver IDs to exclude from traffic checks.
        
    Returns:
        tuple: (adjusted_pace, status_note, passed_driver_id)
    """
    
    # Filter traffic map to current lap and calculate gap to our predicted finish time
    lap_traffic = traffic_map[traffic_map['lap_number'] == current_lap].copy()
    lap_traffic['gap_to_us'] = current_finish_time - lap_traffic['race_time']
    
    # Identify cars ahead within a 2.5s window ahead, or -0.2s window behind, excluding those in the ignore list and ourselves. Sort by proximity.
    cars_ahead = lap_traffic[
        (lap_traffic['gap_to_us'] > -0.2) & 
        (lap_traffic['gap_to_us'] < 2.5) &
        (lap_traffic['driver_number'] != driver_id) & 
        (~lap_traffic['driver_number'].isin(ignore_list))
    ].sort_values('gap_to_us')
    
    if cars_ahead.empty:
        return predicted_pace, "Clean Air", None # No traffic, return original pace
    
    target = cars_ahead.iloc[0]
    target_id = int(target['driver_number'])
    target_finish_time = target['race_time']
    target_pace = target['lap_duration']
    
    # Check for DRS train
    all_traffic = traffic_map[traffic_map['lap_number'] == current_lap]
    target_gaps = target_finish_time - all_traffic['race_time']
    
    # Identify if target car is in a DRS train by checking for cars ahead of it within 2s, excluding ourselves and the target. If so, we require a larger pace advantage to pass.
    train_ahead = all_traffic[
        (target_gaps > 0.0) & 
        (target_gaps < 2.0) & 
        (all_traffic['driver_number'] != target_id) &
        (all_traffic['driver_number'] != driver_id)
    ]
    
    # If the target is in a DRS train, we require a larger pace advantage to successfully pass. If we're close but not quite within the required delta, we get stuck behind and take a penalty. Otherwise, we successfully overtake or chase the target.
    is_in_drs_train = not train_ahead.empty
    pace_delta = target_pace - predicted_pace
    personal_drs_delta, personal_normal_delta = get_overtake_thresholds(target_id)
    
    required_delta = personal_drs_delta if is_in_drs_train else personal_normal_delta
    
    if pace_delta > required_delta:
        status = "Train Break" if is_in_drs_train else "Overtake"
        return predicted_pace + 0.4, f"{status} #{target_id}", target_id
        
    elif pace_delta > -0.2:
        penalty = 0.5 if is_in_drs_train else 0.2
        blocked_pace = max(predicted_pace, target_pace + penalty)
        status = "Stuck in Train" if is_in_drs_train else "Blocked"
        return blocked_pace, f"{status} behind #{target_id}", None
            
    else:
        return predicted_pace, f"Chasing #{target_id}", None

# --- CORE SIMULATION ENGINE ---
def run_simulation(driver_id, session_key, start_lap, end_lap, pit_lap, historic_pit_lap, tire_compound, track_temp, air_temp, pace_bias=0.0, history_map=None):
    """
    Main simulation loop that predicts lap times with traffic, tire degradation, and pit stop logic.
    
    Applies lap-by-lap penalties for tire degradation, traffic, and battles. Uses historical data
    as reference for laps before any pit stops occur.
    
    Args:
        driver_id (int): Driver number.
        session_key (int): Session identifier.
        start_lap (int): Starting lap of simulation.
        end_lap (int): Ending lap of simulation.
        pit_lap (int): Predicted pit stop lap.
        historic_pit_lap (int): Historical pit stop lap for calibration.
        tire_compound (str): New tire compound choice (SOFT, MEDIUM, HARD).
        track_temp (float): Track temperature in Celsius.
        air_temp (float): Air temperature in Celsius.
        pace_bias (float): Bias adjustment from calibration phase.
        history_map (dict): Historical lap data for reference.
        
    Returns:
        pd.DataFrame: Simulated lap times with notes.
    """
    
    # Pre-fetch historical pit loss, traffic map, and pace map for efficient access during simulation
    historic_loss = get_historic_pit_loss(driver_id, session_key)
    traffic_map = build_traffic_map(session_key)
    pace_map = build_historic_pace_map(session_key)
    
    # Thresholds for tire degredation
    TIRE_LIMITS = {
        'SOFT': 18,
        'MEDIUM': 28,
        'HARD': 45,    
        'INTERMEDIATE': 30,
        'WET': 30
    }
    
    query = text(f"SELECT SUM(lap_duration) FROM ml_training_data WHERE session_key={session_key} AND driver_number={driver_id} AND lap_number < {start_lap}")
    with engine.connect() as conn: 
        start_time = conn.execute(query).scalar() or 0.0
        
    current_race_time = start_time
    
    # Load the model
    if hasattr(rf_model, "feature_names_in_"):
        correct_cols = rf_model.feature_names_in_
    else:
        correct_cols = MODEL_COLS

    active_battles = {}
    passed_cars_memory = set()
    results = []
    
    # Initialize virtual tire age and compound based on historical data at the starting lap, or default to 1 lap old HARD if no data is available. This allows the simulation to start with a realistic tire state.
    if start_lap in history_map:
        virtual_tire_age = history_map[start_lap]['tire_age']
        current_virtual_compound = history_map[start_lap]['tire_compound'] 
    else:
        virtual_tire_age = 1
        current_virtual_compound = 'HARD'

    # Simulate lap by lap, applying historical data where available before the pit stop, and model predictions with penalties after the pit stop. Update virtual tire state and track traffic conditions dynamically.
    for lap in range(start_lap, end_lap + 1):
        note = ""
        is_pit_lap = (lap == pit_lap)
        should_use_history = (lap < pit_lap) and (lap < historic_pit_lap)
        
        # For laps before the pit stop, use historical data if available to ensure the simulation starts with a realistic baseline. After the pit stop, rely on model predictions and apply penalties for tire degradation and traffic. This approach allows us to leverage real data where it matters most for calibration, while still simulating the strategic impact of the new tire choice and pit timing.
        if should_use_history and (lap in history_map):
            final_time = history_map[lap]['lap_duration']
            current_virtual_compound = history_map[lap]['tire_compound']
            virtual_tire_age = history_map[lap]['tire_age']
            note = "Historic Data"
        
        else:
            # After the pit stop, we assume the driver is on the new tire compound. For laps before the pit stop, we keep the virtual tire state consistent with historical data to maintain accuracy in the early part of the simulation.
            if lap > pit_lap:
                current_virtual_compound = tire_compound

            # Create a dictionary with all 0s for the expected columns
            input_dict = {col: 0 for col in correct_cols}
            
            # Fill in the continuous features
            input_dict['laps_on_tire'] = virtual_tire_age
            input_dict['fuel_proxy'] = -1 * lap
            input_dict['track_temperature'] = track_temp
            input_dict['air_temperature'] = air_temp
            input_dict['rainfall'] = 0  # Assuming 0 if not explicitly tracked
            
            # Fill in the One-Hot Encoded categorical features
            if f"driver_number_{driver_id}" in input_dict:
                input_dict[f"driver_number_{driver_id}"] = 1
            if f"tire_compound_{current_virtual_compound}" in input_dict:
                input_dict[f"tire_compound_{current_virtual_compound}"] = 1
            
            # Convert to DataFrame and enforce correct column order for the model
            # Ensures scaler and model receive data
            input_df = pd.DataFrame([input_dict])[correct_cols]

            # Scale the data
            input_scaled = MODEL_SCALER.transform(input_df)

            # Predict and get the sum (total lap time)
            raw_pace = rf_model.predict(input_scaled)[0].sum()
            base_time = raw_pace - pace_bias
            
            # Tire degradation penalty
            deg_penalty = 0.0
            limit = TIRE_LIMITS.get(current_virtual_compound, 30)
            if virtual_tire_age > limit:
                excess_laps = virtual_tire_age - limit
                deg_penalty = 0.08 * (excess_laps ** 1.6)
                if deg_penalty > 0.5:
                    note += f" [Degradation: +{deg_penalty:.1f}s]"

            # Re-pass penalty
            battle_penalty = 0.0
            for opp_id in list(active_battles.keys()):
                pass_lap = active_battles[opp_id]
                if (lap - pass_lap) > 2:
                    del active_battles[opp_id]
                    passed_cars_memory.add(opp_id)
                    continue
                
                opp_pace = pace_map.get(opp_id, {}).get(lap, 999.0)
                if (base_time - opp_pace) > 0.2:
                    battle_penalty += 0.8
                    note += f" Re-passed by #{opp_id}!"
                    del active_battles[opp_id]
                    if opp_id in passed_cars_memory:
                        passed_cars_memory.remove(opp_id)
                else:
                    battle_penalty += 0.05

            tentative_time = base_time + battle_penalty + deg_penalty # Apply all penalties to the base time to get the tentative lap time before traffic adjustments for total predicted time
            
            # If the current lap is the pit stop lap, we apply the historical pit loss penalty and check for traffic upon exit
            # For non-pit laps, we check for traffic based on the predicted finish time and adjust the lap time accordingly
            if is_pit_lap:
                final_time = tentative_time + historic_loss
                note = f"PIT STOP (+{historic_loss:.1f}s)"
                
                estimated_exit_time = current_race_time + final_time # Current predicted finish time after applying pit loss
                ignore_list = list(passed_cars_memory)
                
                # Check traffic on pit lane exit
                _, traffic_note, _ = check_traffic(
                    lap, estimated_exit_time, tentative_time, driver_id, traffic_map, ignore_list
                )
                
                # Penalise race time if in traffic
                if "Stuck" in traffic_note or "Blocked" in traffic_note:
                    final_time += 1.0
                    note += " (Exited into Traffic +1.0s)"
                
                virtual_tire_age = 0 # Reset virtual tire age after pit stop
                
            else:
                # For non-pit laps, we check for traffic based on the predicted finish time and adjust the lap time accordingly 
                # We also track active battles and passed cars to simulate dynamic on-track interactions throughout the stint
                ignore_list = list(passed_cars_memory)
                predicted_finish_time = current_race_time + tentative_time
                
                final_time, traffic_note, passed_id = check_traffic(
                    lap, predicted_finish_time, tentative_time, driver_id, traffic_map, ignore_list
                )
                
                if "Clean" not in traffic_note:
                    note = f"{note} {traffic_note}".strip()
                if passed_id:
                    active_battles[passed_id] = lap

        current_race_time += final_time # Update cumulative race time
        virtual_tire_age += 1 # Increment virtual tire age for the next lap
        
        # Store results for this lap with all notes on traffic, battles, and degradation for later analysis and visualization
        results.append({
            "Lap": lap,
            "Time": round(final_time, 3),
            "CumTime": round(current_race_time, 2),
            "Note": note.strip()
        })

    return pd.DataFrame(results)

# --- CALIBRATION & ACCURACY ---
def calculate_model_accuracy(simulation_df, session_key, driver_id):
    """
    Compares simulated lap times to actual lap times and calculates error metrics.
    
    Args:
        simulation_df (pd.DataFrame): Simulated lap data.
        session_key (int): Session identifier.
        driver_id (int): Driver number.
        
    Returns:
        tuple: (comparison_df, metrics_dict)
    """
    start_lap = simulation_df['Lap'].min()
    end_lap = simulation_df['Lap'].max()
    
    query = text(f"""
        SELECT lap_number as Lap, lap_duration as Actual_Time
        FROM ml_training_data 
        WHERE session_key = {session_key} 
        AND driver_number = {driver_id}
        AND lap_number BETWEEN {start_lap} AND {end_lap}
    """)
    
    with engine.connect() as conn:
        actual_df = pd.read_sql(query, conn)
    
    # Merge Historic and Simulated Data
    comparison_df = pd.merge(simulation_df, actual_df, on='Lap', how='left')
    valid_comparison = comparison_df.dropna(subset=['Actual_Time', 'Time'])
    
    if valid_comparison.empty:
        return comparison_df, {"MAE": 0, "RMSE": 0, "Delta": 0, "Confidence_Score": 0} # No valid data to compare, return default metrics
    
    # Calculate error metrics to evaluate model performance
    mae = mean_absolute_error(valid_comparison['Actual_Time'], valid_comparison['Time'])
    rmse = np.sqrt(mean_squared_error(valid_comparison['Actual_Time'], valid_comparison['Time']))
    
    total_sim_time = valid_comparison['Time'].sum()
    total_actual_time = valid_comparison['Actual_Time'].sum()
    delta = total_sim_time - total_actual_time
    number_of_laps = len(valid_comparison)
    confidence_score = max(0, 100 - mae * number_of_laps * 2)
    
    metrics = {
        "MAE": mae,
        "RMSE": rmse,
        "Total_Delta": delta,
        "Confidence_Score": confidence_score
    }
    
    return comparison_df, metrics

def calibrate_and_simulate(driver_id, session_key, start_lap, end_lap, pit_lap, historic_pit_lap, tire_compound, track_temp, air_temp, historic_map):
    """
    Two-phase simulation: calibration for bias, then final strategy with corrected pace.
    
    Args:
        driver_id (int): Driver number.
        session_key (int): Session identifier.
        start_lap (int): Starting lap.
        end_lap (int): Ending lap.
        pit_lap (int): New pit stop lap.
        historic_pit_lap (int): Historical pit stop lap.
        tire_compound (str): New tire compound.
        track_temp (float): Track temperature.
        air_temp (float): Air temperature.
        historic_map (dict): Historical lap data.
        
    Returns:
        tuple: (final_df, bias_used)
    """
    post_pit_lap = historic_pit_lap - 1
    
    if post_pit_lap in historic_map:
        historic_compound = historic_map[post_pit_lap]['tire_compound']
    else:
        historic_compound = tire_compound

    # Calibration phase
    control_df = run_simulation(
        driver_id, session_key, start_lap, end_lap, 
        pit_lap=historic_pit_lap, 
        historic_pit_lap=historic_pit_lap,
        tire_compound=historic_compound, track_temp=track_temp, air_temp=air_temp, 
        pace_bias=0.0, # No bias during calibration to measure pure model error against historical data
        history_map=historic_map
    )
    
    query = text(f"""
        SELECT lap_number as "Lap", lap_duration as "Actual_Time"
        FROM ml_training_data 
        WHERE session_key={session_key} 
        AND driver_number={driver_id} 
        AND lap_number BETWEEN {start_lap} AND {end_lap}
        AND lap_duration IS NOT NULL
    """)
    
    with engine.connect() as conn:
        actual_df = pd.read_sql(query, conn)
    
    # bias is the difference of simulated and actual cumulative race time.
    merged_df = pd.merge(control_df, actual_df, on='Lap', how='inner')
    
    if not merged_df.empty:
        merged_df['delta'] = merged_df['Time'] - merged_df['Actual_Time']
        bias = merged_df['delta'].median()
    else:
        bias = 0.0

    # Strategy phase (Feed bias back into the model)
    final_df = run_simulation(
        driver_id, session_key, start_lap, end_lap, 
        pit_lap=pit_lap, 
        historic_pit_lap=historic_pit_lap,
        tire_compound=tire_compound, track_temp=track_temp, air_temp=air_temp, 
        pace_bias=bias, # Apply bias correction from calibration to adjust model predictions for a more accurate strategy simulation
        history_map=historic_map
    )
    
    return final_df, bias

# --- TELEMETRY & GHOST LAP GENERATION ---
@st.cache_data
def get_track_map_image(key):
    """Fetches static track layout."""
    return raceData.get_track_layout(key)

@st.cache_data
def get_race_data(key):
    """Fetches and processes race replay data for visualization."""
    resampled, lap_times = raceData.get_race_replay_data(key)
    if resampled is None or (hasattr(resampled, 'empty') and resampled.empty):
        return pd.DataFrame(), pd.DataFrame()
    
    pit_data = mlData.fetchMLData(key)
    stints = []
    
    # Process pit stop data to determine stints and tire compounds for each driver
    if not pit_data.empty:
        if 'driver_number' in resampled.columns and 'driver_acronym' in resampled.columns:
            driver_map = resampled[['driver_number', 'driver_acronym']].drop_duplicates()
            pit_data['driver_number'] = pit_data['driver_number'].astype(driver_map['driver_number'].dtype)
            pit_data = pit_data.merge(driver_map, on='driver_number', how='left')
        else:
            pit_data['driver_acronym'] = pit_data['driver_number'].astype(str)

        y_order = pit_data['driver_acronym'].dropna().unique().tolist()
        lap_col = 'lap_number'
        compound_col = 'tire_compound'
        laps_on_tire_col = 'laps_on_tire'

        # Iterate through each driver to identify stints based on pit stop data, tracking tire compounds and laps on tire
        for drv in y_order:
            ddf = pit_data[pit_data['driver_acronym'] == drv].copy().sort_values(lap_col)
            if lap_col not in ddf.columns or compound_col not in ddf.columns:
                continue
            
            current_comp = None
            current_start = None
            prev_lap = None
            prev_tire_age = None

            for _, r in ddf.iterrows():
                lap = int(r[lap_col])
                comp = str(r[compound_col]).upper() if pd.notna(r[compound_col]) else 'UNKNOWN'
                current_tire_age = int(r[laps_on_tire_col]) if laps_on_tire_col in ddf.columns and pd.notna(r[laps_on_tire_col]) else (999 if prev_tire_age is None else prev_tire_age + 1) # Increment tire age if not explicitly provided, or set to 999 if we have no prior data to assume it's very old

                # Start a new stint if we encounter a different compound, if the tire age decreases (indicating a pit stop), or if there is a gap in laps (indicating missing data or a pit stop)
                if current_comp is None:
                    current_comp = comp
                    current_start = lap
                elif (comp != current_comp) or (prev_tire_age is not None and current_tire_age < prev_tire_age) or (prev_lap is not None and lap > prev_lap + 1):
                    stints.append({'driver': drv, 'start': current_start, 'end': prev_lap, 'compound': current_comp})
                    current_comp = comp
                    current_start = lap
                
                prev_lap = lap
                prev_tire_age = current_tire_age
            
            if current_comp is not None:
                stints.append({'driver': drv, 'start': current_start, 'end': prev_lap, 'compound': current_comp}) # Close out the final stint for the driver

    # Process stints with compound, lap and driver information
    stint_lap_data = []
    for s in stints:
        if s['start'] is not None and s['end'] is not None:
            for lap in range(s['start'], s['end'] + 1):
                stint_lap_data.append({'driver_acronym': s['driver'], 'lap_number': lap, 'compound': s['compound']})
    
    stints_df = pd.DataFrame(stint_lap_data)
    df = resampled
    
    # Merge stint data with the main telemetry data to annotate each lap with the tire compound used by each driver
    if not stints_df.empty:
        df = pd.merge(df, stints_df, on=['driver_acronym', 'lap_number'], how='left')
        df['compound'] = df['compound'].fillna('Unknown')
    else:
        df['compound'] = 'Unknown'

    # Fetch driver colors and merge with telemetry data for visualization
    # If no colors are available, default to a standard color
    df_colors = raceData.get_driver_colors(key)
    if not df_colors.empty:
        df = pd.merge(df, df_colors, on='driver_acronym', how='left')
        df['team_colour'] = df['team_colour'].fillna('#FF1508')
        lap_times = pd.merge(lap_times, df_colors[['driver_acronym','team_colour']], on='driver_acronym', how='left')
        lap_times['team_colour'] = lap_times['team_colour'].fillna('#FF1508')
    else:
        df['team_colour'] = '#FF1508'
        lap_times['team_colour'] = '#FF1508'
    
    # Inbuilt function to convert raw lap time to mm:ss.mmm format
    def fmt_time_seconds(val):
        try:
            t = float(val)
        except Exception:
            return ''
        mins = int(t // 60)
        secs = int(t % 60)
        millis = int(round((t - int(t)) * 1000))
        return f"{mins}:{secs:02d}.{millis:03d}"

    if not lap_times.empty:
        lap_times['lap_time_fmt'] = lap_times['lap_time'].apply(fmt_time_seconds)
    else:
        lap_times['lap_time_fmt'] = []
    
    # Process telemetry data to create a unified timeline for all drivers, interpolating X,Y coordinates to align with a master timeline
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    start_time = df['timestamp'].min()
    df['race_time'] = (df['timestamp'] - start_time).dt.total_seconds()
    df = df.sort_values('race_time')

    # Calculate lap start times for each driver and lap to align telemetry data on a common timeline
    lap_start_times = df.groupby(['driver_acronym', 'lap_number'])['race_time'].min().reset_index()
    lap_start_times.rename(columns={'race_time': 'lap_start_time'}, inplace=True)
    df = pd.merge(df, lap_start_times, on=['driver_acronym', 'lap_number'], how='left')

    min_t = df['race_time'].min()
    max_t = df['race_time'].max()
    master_timeline = np.arange(min_t, max_t, 1.0) # 1-second intervals for interpolation
    
    aligned_dfs = []
    
    # For each driver, we create a complete master timeline of their telemetry data, interpolating X,Y coordinates to fill in any gaps and align with the master timeline
    for driver in df['driver_acronym'].unique():
        d_data = df[df['driver_acronym'] == driver].set_index('race_time')
        d_data = d_data[~d_data.index.duplicated(keep='first')]
        
        d_color = d_data['team_colour'].iloc[0] if 'team_colour' in d_data.columns else '#FF1508'
        
        union_index = d_data.index.union(master_timeline)
        d_interp = d_data.reindex(union_index)
        
        d_interp['x'] = d_interp['x'].interpolate(method='slinear', limit_direction='both')
        d_interp['y'] = d_interp['y'].interpolate(method='slinear', limit_direction='both')
        
        d_interp = d_interp.ffill().bfill()
        d_interp = d_interp.reindex(master_timeline)
        d_interp['driver_acronym'] = driver
        d_interp['team_colour'] = d_color
        
        aligned_dfs.append(d_interp.reset_index().rename(columns={'index': 'race_time'}))
        
    unified_df = pd.concat(aligned_dfs)
    unified_df['lap_number'] = unified_df['lap_number'].fillna(0).astype(int)
    
    return unified_df, lap_times

def fetch_driver_sector_times_and_position(session_key, driver_id):
    """
    Fetches sector times and telemetry coordinates for a reference lap.
    Selects the lap closest to the driver's average race pace.
    
    Args:
        session_key (int): Session identifier.
        driver_id (int): Driver number.
        
    Returns:
        pd.DataFrame: Telemetry data with sector times.
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
    Generates predicted coordinates and timestamps for a ghost lap.
    
    Distributes predicted time across sectors based on reference lap ratios,
    then interpolates X,Y coordinates from the reference geometry.
    
    Args:
        ref_df (pd.DataFrame): Reference lap telemetry with sector times.
        predicted_sectors (dict): Predicted sector durations {duration_sector_1, 2, 3}.
        driver_id (int): Driver number.
        lap_number (int): Lap number.
        
    Returns:
        pd.DataFrame: Ghost lap with x, y, lap_number, driver_id.
    """
    
    # Calculate total reference lap time and sector ratios to distribute predicted time across the lap
    ref_s1 = ref_df['duration_sector_1'].iloc[0]
    ref_s2 = ref_df['duration_sector_2'].iloc[0]
    ref_s3 = ref_df['duration_sector_3'].iloc[0]
    ref_total = ref_s1 + ref_s2 + ref_s3
    
    # Determine how many telemetry points fall into each sector based on the reference lap's sector time ratios, then create new timestamps for the ghost lap by distributing the predicted sector times across these points
    n_points = len(ref_df)
    idx_s1 = int(n_points * (ref_s1 / ref_total))
    idx_s2 = int(n_points * ((ref_s1 + ref_s2) / ref_total))
    
    # Create new timestamps for the ghost lap by distributing the predicted sector times across the telemetry points according to the reference lap's sector time ratios
    t_s1 = np.linspace(0, predicted_sectors['duration_sector_1'], num=idx_s1, endpoint=False)
    start_s2 = predicted_sectors['duration_sector_1']
    end_s2 = start_s2 + predicted_sectors['duration_sector_2']
    t_s2 = np.linspace(start_s2, end_s2, num=(idx_s2 - idx_s1), endpoint=False)
    
    # For the final sector, we continue the timeline from the end of sector 2 to the total predicted lap time, ensuring that the ghost lap's timestamps align with the predicted sector durations while following the reference lap's geometry.
    start_s3 = end_s2
    end_s3 = start_s3 + predicted_sectors['duration_sector_3']
    t_s3 = np.linspace(start_s3, end_s3, num=(n_points - idx_s2))
    
    new_race_time = np.concatenate([t_s1, t_s2, t_s3]) # Combine all sector timestamps to create a complete timeline for the ghost lap
    
    # New DataFrame for interpolated telemtry
    ghost_df = ref_df[['x', 'y']].copy()
    ghost_df['race_time'] = new_race_time
    ghost_df['lap_number'] = lap_number
    ghost_df['driver_id'] = driver_id + "_PRED"
    
    return ghost_df

def fetch_pit_lap_telemetry(session_key, driver_id, lap_number):
    """Fetches telemetry for a specific pit lap."""
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

def simulate_stint(session_id, driver_id, pit_lap, historic_pit_lap, tire_compound, start_lap, end_lap):
    """
    Generates a complete stint simulation with ghost lap telemetry.
    
    Combines simulation results with historical telemetry to create animated replay data.
    Automatically swaps between racing and pit lane geometries based on lap context.
    
    Args:
        session_id (int): Session identifier.
        driver_id (int): Driver number.
        pit_lap (int): Predicted pit stop lap.
        historic_pit_lap (int): Historical pit stop lap.
        tire_compound (str): New tire compound.
        start_lap (int): Starting lap.
        end_lap (int): Ending lap.
        
    Returns:
        pd.DataFrame: Ghost lap telemetry with all metadata.
    """
    query = text(f"""
        SELECT track_temperature, air_temperature
        FROM ml_training_data 
        WHERE session_key={session_id}
    """)
    
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
    
    history_map = history_df.set_index('lap_number').to_dict('index')
    
    # Set parameter with historic data
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
        "historic_map": history_map
    }
    
    sim_df, bias_used = calibrate_and_simulate(**params)
    
    # Calculate accuracy against the CONTROL (historical) race, not the alternate strategy
    control_sim_df = run_simulation(
        driver_id, session_id, start_lap, end_lap,
        pit_lap=historic_pit_lap,
        historic_pit_lap=historic_pit_lap,
        tire_compound=history_map.get(historic_pit_lap - 1, {}).get('tire_compound', tire_compound),
        track_temp=track_temp,
        air_temp=air_temp,
        pace_bias=bias_used,
        history_map=history_map
    )
    acc_df, metrics = calculate_model_accuracy(control_sim_df, session_id, driver_id)
    
    # User Interface updates
    render_hero_card("Calculating...")
    
    with st.status("Running Predictive Model...", expanded=True) as status:
        st.write("Applying Pace Bias...")
        st.write("Calculating Overtake Deltas...")
        st.session_state['model_accuracy'] = metrics['Confidence_Score']
        status.update(label="Simulation Complete!", state="complete")
    
    render_hero_card(st.session_state['model_accuracy'])
    
    # Fetch telemetry
    ref_racing = fetch_driver_sector_times_and_position(session_id, driver_id)
    ref_pitting = fetch_pit_lap_telemetry(session_id, driver_id, historic_pit_lap)
    
    if ref_racing.empty or ref_pitting.empty or sim_df.empty:
        return pd.DataFrame()

    ghost_laps_list = []
    
    # For each simulated lap, we determine whether it's a pit stop lap or a racing lap, select the appropriate reference telemetry, and generate a ghost lap by distributing the predicted sector times across the reference lap's geometry
    # We then adjust the ghost lap's timestamps to align with the predicted total lap time and cumulative race time, ensuring that the ghost lap accurately reflects the predicted performance while following the realistic track layout and sector characteristics of the reference laps
    for _, row in sim_df.iterrows():
        lap_num = int(row['Lap'])
        pred_total_time = row['Time']
        
        if lap_num == pit_lap:
            current_ref = ref_pitting # Pit lap telemetry
        else:
            current_ref = ref_racing # Racing lap telemetry (closest to average pace)
            
        ref_s1 = current_ref['duration_sector_1'].iloc[0]
        ref_s2 = current_ref['duration_sector_2'].iloc[0]
        ref_s3 = current_ref['duration_sector_3'].iloc[0]
        ref_total = ref_s1 + ref_s2 + ref_s3
        
        predicted_sectors = {
            'duration_sector_1': pred_total_time * (ref_s1 / ref_total),
            'duration_sector_2': pred_total_time * (ref_s2 / ref_total),
            'duration_sector_3': pred_total_time * (ref_s3 / ref_total)
        }
        
        ghost_lap = generate_ghost_lap(current_ref, predicted_sectors, driver_id=str(driver_id), lap_number=lap_num)
        
        lap_start_time = row['CumTime'] - pred_total_time
        ghost_lap['race_time'] = ghost_lap['race_time'] + lap_start_time
        ghost_lap['lap_number'] = lap_num
        ghost_lap['lap_duration'] = pred_total_time
        ghost_lap['lap_start_time'] = lap_start_time
        
        ghost_laps_list.append(ghost_lap)
        
    final_ghost = pd.concat(ghost_laps_list, ignore_index=True)
    final_ghost['driver_acronym'] = 'GHOST'
    final_ghost['team_colour'] = "#757576"
    final_ghost['driver_number'] = driver_id
    final_ghost['compound'] = tire_compound
    final_ghost['team_name'] = "Simulated Ghost"
    
    return final_ghost

# --- USER INTERFACE ---
def start_simulation(session_key):
    """Main UI orchestration for strategy simulation."""
    with st.spinner(f"Optimizing {race_name} Data"):
        df, lap_times_df = get_race_data(session_key)
        track_df = get_static_track(session_key)

        if df.empty or track_df is None:
            st.error("Data unavailable.")
            return

        st.markdown("<div class='card-title' style='margin:14px 0 4px;'>Performance & Strategy</div>", unsafe_allow_html=True)
        left_col, right_col = st.columns([0.5, 0.5])

        with left_col:
            st.markdown("<div style='text-align:center; font-size:16px; font-weight:600; margin-bottom:4px;'>Driver Selection</div>", unsafe_allow_html=True)
            
            selected_driver = st.selectbox("Select Driver for Strategy Simulation", options=sorted(df['driver_acronym'].unique()), index=0)
            driver_id_query = text(f"SELECT DISTINCT driver_number FROM race_telemetry WHERE session_key={session_key} AND driver_acronym='{selected_driver}'")
            with engine.connect() as conn:
                driver_id = conn.execute(driver_id_query).scalar() or None
                
            # Fetch the most recent pit stop lap
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
            
            # Determine lap range for pit stop selection based on historical pit lap for upper bound
            max_lap_query = text(f"""
                SELECT MAX(lap_number) 
                FROM ml_training_data 
                WHERE session_key={session_key} 
                  AND driver_number={driver_id}
            """)
            with engine.connect() as conn:
                max_lap_number = conn.execute(max_lap_query).scalar()
                
            # Set upper and lower bound for pit lap selection
            max_pit_lap = max_lap_number - 1 if historic_pit_lap + 10 > max_lap_number else historic_pit_lap + 10
            min_pit_lap = 1 if historic_pit_lap - 10 < 1 else historic_pit_lap - 10
            
            selected_pit_lap = st.number_input("Select Alternate Pit Stop Lap", min_value=min_pit_lap, max_value=max_pit_lap, value=historic_pit_lap, step=1)
        
            earliest_event_lap = min(historic_pit_lap, selected_pit_lap)
            start_lap = max(1, earliest_event_lap - 1)
            
            # Fetch the most recent tire ompound used for the historic pit stop lap
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
            
            # If no valid tire compound data is available, show an error and prevent simulation
            if historic_tire_compound == 'UNKNOWN':
                st.error("No valid tire compound data available for this driver. Simulation cannot proceed. Please select a different driver or check data integrity.")
                return
            
            # Default selected tire to historic compound, but allow user to change it for alternate scenarios
            compound_options = ['SOFT', 'MEDIUM', 'HARD']
            selected_tire_compound = st.selectbox("Select Tire Compound", options=compound_options, index=compound_options.index(historic_tire_compound))

                
            if st.button("Run Strategy Simulation"):  
                st.session_state['button_clicked'] = True
            else:
                st.session_state['button_clicked'] = False
                
        # Run simulation and render results only if button was clicked to prevent automatic execution on every selection change
        if st.session_state.get('button_clicked', True):
            with st.spinner("Running Strategy Simulation"):
                ghost_lap_data = simulate_stint(
                    session_id=session_key,
                    driver_id=driver_id,
                    pit_lap=selected_pit_lap,
                    historic_pit_lap=historic_pit_lap,
                    tire_compound=selected_tire_compound,
                    start_lap=start_lap,
                    end_lap=max_lap_number
                )
                
                if ghost_lap_data.empty:
                    st.error("Simulation failed due to missing telemetry data.")
                    return
                
                with right_col:
                    # Render lap time comparison chart between historical lap times and predicted lap times for the alternate strategy, annotating with tire compounds and pit stop information for context
                    st.markdown("<div style='text-align:center; font-size:16px; font-weight:600; margin-bottom:4px;'>Lap Time Comparison</div>", unsafe_allow_html=True)

                    hist_query = text(f"""
                        SELECT lap_number, lap_duration
                        FROM ml_training_data 
                        WHERE session_key = {session_key} 
                          AND driver_number = {driver_id}
                          AND lap_number BETWEEN {start_lap - 10} AND {max_lap_number}
                        ORDER BY lap_number
                    """)
                    
                    with engine.connect() as conn:
                        historic_lap_times = pd.read_sql(hist_query, conn)

                    # Calculate predicted lap times from the ghost lap data by taking the difference between the maximum and minimum race time
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
                    
                    lap_fig = go.Figure()
                    
                    # Add historical lap times to the chart, annotating each point with the active tire compound and the compound used before the pit stop for context, allowing users to see how tire strategy impacted lap times historically
                    if not historic_lap_times.empty:
                        hist_label = f"Active Compound: {historic_tire_compound}<br>Compound before pit: {history_previous_tire_compound}"
                        hist_customdata = [hist_label] * len(historic_lap_times)
                        
                        lap_fig.add_trace(go.Scatter(
                            x=historic_lap_times['lap_number'], 
                            y=historic_lap_times['lap_duration'],
                            mode='lines+markers',
                            name='Historic Lap Time',
                            line=dict(color='blue'),
                            marker=dict(size=6),
                            customdata=hist_customdata,
                            hovertemplate='Lap %{x}<br>Time: %{y:.3f}s<br>%{customdata}<extra></extra>',
                        ))
                    
                    # Add predicted lap times to the chart with similar annotations for the active tire compound and the compound used before the pit stop, 
                    # allowing users to directly compare the predicted performance of the alternate strategy against historical data while understanding the context of tire usage
                    if not predicted_lap_times_df.empty:
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
                        ))

                    lap_fig.update_layout(
                        title=f"Lap Time Comparison for {selected_driver}",
                        xaxis_title="Lap Number",
                        yaxis_title="Lap Time (seconds)",
                        template="plotly_white",
                        height=400
                    )
                    st.plotly_chart(lap_fig, width='stretch')
                    
                st.markdown("<div style='margin-top:24px;'></div>", unsafe_allow_html=True)
                left_col, right_col = st.columns([0.5, 0.5]) # Set up two columns for the strategy replay and the lap time comparison chart
                
                with left_col:
                    # Render an animated replay of the race, overlaying the ghost lap telemetry on top of the actual track layout
                    st.markdown("<div style='text-align:center; font-size:16px; font-weight:600; margin-top:16px; margin-bottom:4px'>Strategy Replay</div>", unsafe_allow_html=True)
                    
                    replay_start = start_lap
                    replay_end = max_lap_number
                    needed_cols = ['x', 'y', 'driver_acronym', 'team_colour', 'race_time', 'lap_number', 'compound', 'lap_start_time']
                    
                    # Extract the relevant telemetry data for the real stint and the ghost lap, ensuring we have all necessary columns for visualization and animation
                    real_stint = df.loc[(df['lap_number'] >= replay_start) & (df['lap_number'] <= replay_end), needed_cols].copy()
                    ghost_stint = ghost_lap_data[needed_cols].copy()

                    # To ensure the ghost lap aligns correctly with the real telemetry, we adjust the ghost lap's timestamps to match the real lap's start time, allowing for a seamless overlay of the ghost lap onto the actual track layout during the animation
                    if not real_stint.empty and not ghost_stint.empty:
                        real_start_time = real_stint[real_stint['driver_acronym'] == selected_driver]['race_time'].min()
                        ghost_start_time = ghost_stint['race_time'].min()
                        time_offset = real_start_time - ghost_start_time
                        ghost_stint['race_time'] = ghost_stint['race_time'] + time_offset

                    all_data = pd.concat([real_stint, ghost_stint], ignore_index=True)
                    
                    # To create a smooth animation, we need to interpolate the telemetry data onto a master timeline.
                    # We calculate the minimum and maximum race time across all telemetry data to determine the total duration of the stint, 
                    # then create a master timeline with a target number of frames (e.g., 800) to ensure smooth animation while balancing performance.
                    # We use interpolation to fill in any gaps in the telemetry data, allowing for a continuous and fluid replay of the race, even if the original telemetry has irregular timestamps or missing data points.
                    # This process ensures that the ghost lap and the real telemetry are perfectly synchronized on the same timeline for accurate visualization
                    if not all_data.empty:
                        min_time = all_data['race_time'].min()
                        max_time = all_data['race_time'].max()
                    else:
                        min_time, max_time = 0, 1

                    total_duration = max_time - min_time
                    if total_duration <= 0:
                        total_duration = 1
                    
                    target_frames = 800
                    calculated_step = total_duration / target_frames
                    step_size = max(0.2, calculated_step) # Ensure a minimum step size to prevent excessive frames for very short stints
                    master_timeline = np.arange(min_time, max_time, step_size)
                    
                    interpolated_frames = []
                    unique_drivers = sorted(all_data['driver_acronym'].unique())
                    
                    # For each driver, we interpolate their X and Y coordinates onto the master timeline using either Akima interpolation for smoother curves (if we have enough data points) or linear interpolation as a fallback, ensuring that the animation will have smooth and accurate trajectories for each driver, even if the original telemetry data is sparse or irregularly spaced.
                    for driver in unique_drivers:
                        d_data = all_data[all_data['driver_acronym'] == driver].sort_values('race_time')
                        d_data = d_data.drop_duplicates(subset=['race_time'])
                        
                        if len(d_data) < 2:
                            continue

                        team_color = d_data['team_colour'].iloc[0]
                        times = d_data['race_time'].values
                        x_vals = d_data['x'].values
                        y_vals = d_data['y'].values
                        laps = d_data['lap_number'].values

                        # Try to use Akima interpolation for smoother trajectories, but if there are too few data points or if it fails for any reason, fall back to linear interpolation to ensure we still get a complete set of coordinates for the animation, prioritizing robustness and continuity in the visualization even if the original telemetry data is limited.
                        try:
                            if len(d_data) > 3:
                                akima_x = Akima1DInterpolator(times, x_vals)
                                akima_y = Akima1DInterpolator(times, y_vals)
                                new_x = akima_x(master_timeline)
                                new_y = akima_y(master_timeline)
                            else:
                                new_x = np.interp(master_timeline, times, x_vals)
                                new_y = np.interp(master_timeline, times, y_vals)
                        except Exception:
                            new_x = np.interp(master_timeline, times, x_vals)
                            new_y = np.interp(master_timeline, times, y_vals)
                        
                        new_laps = np.interp(master_timeline, times, laps)
                        new_laps = np.floor(new_laps).astype(int)
                        
                        # We create a new DataFrame for the interpolated telemetry of each driver
                        d_frame = pd.DataFrame({
                            'race_time': master_timeline,
                            'x': new_x,
                            'y': new_y,
                            'driver_acronym': driver,
                            'team_colour': team_color,
                            'lap_number': new_laps
                        })
                        interpolated_frames.append(d_frame)
                    
                    # After interpolating the telemetry data for all drivers, we concatenate the individual DataFrames into a single DataFrame for the animation
                    if interpolated_frames:
                        animation_df = pd.concat(interpolated_frames, ignore_index=True)
                        animation_df = animation_df.sort_values(['race_time', 'driver_acronym'])
                        animation_df['frame_time'] = animation_df['race_time'].round(1)
                        timestamps_to_animate = animation_df['frame_time'].unique()
                    else:
                        timestamps_to_animate = []

                    replay_fig_key = f"prediction_replay_{session_key}_{selected_driver}_{selected_pit_lap}"
                    
                    # To optimize performance and prevent unnecessary re-computation of the replay figure, we check if a figure with the same parameters already exists in the session state. 
                    # If it does, we reuse it; if not, we generate a new figure using Plotly, creating an animated scatter plot that shows the trajectories of the drivers over time, with controls for play, pause, 
                    # and speed adjustment to allow users to analyze the predicted strategy in detail. 
                    if replay_fig_key in st.session_state:
                        replay_fig = st.session_state[replay_fig_key]
                    elif len(timestamps_to_animate) > 0:
                        replay_fig = go.Figure()

                        padding = 200
                        x_min, x_max = track_df['x'].min() - padding, track_df['x'].max() + padding
                        y_min, y_max = track_df['y'].min() - padding, track_df['y'].max() + padding
                        hud_x = (x_min + x_max) / 2
                        hud_y = y_max - (y_max - y_min) * 0.05

                        # Add track layout as a static line plot for thr background
                        replay_fig.add_trace(go.Scatter(
                            x=track_df['x'], y=track_df['y'], 
                            mode='lines', 
                            line=dict(color="#333", width=6), 
                            hoverinfo='skip'
                        ))

                        start_t = timestamps_to_animate[0]
                        start_data = animation_df[animation_df['frame_time'] == start_t]
                        
                        # We add the initial positions of the drivers at the start of the replay as a scatter plot, using their team colors for the markers and annotating with their acronyms for easy identification.
                        replay_fig.add_trace(go.Scatter(
                            x=start_data['x'], y=start_data['y'], 
                            mode='markers+text', 
                            text=start_data['driver_acronym'],
                            ids=start_data['driver_acronym'], 
                            textposition='top center', 
                            textfont=dict(size=11, color='white', weight='bold'),
                            marker=dict(color=start_data['team_colour'], size=12, line=dict(width=1, color='white')),
                            hovertemplate="%{text}",
                            name='Drivers'
                        ))

                        # We also add a text annotation to serve as a simple HUD, displaying the current lap number in the center of the screen, which updates dynamically as the animation progresses
                        replay_fig.add_trace(go.Scatter(
                            x=[hud_x], 
                            y=[hud_y],
                            mode="text",
                            text=[f"Lap {replay_start}"],
                            textfont=dict(size=20, color="#e5e7eb"),
                            hoverinfo="skip"
                        ))

                        # Render each frame of the animation by filtering the interpolated telemetry for the current timestamp, determining the current lap number, and creating a new frame that updates the positions of the drivers and the HUD annotation accordingly.
                        frames = []
                        for t in timestamps_to_animate:
                            frame_data = animation_df[animation_df['frame_time'] == t]
                            curr_lap = int(frame_data['lap_number'].max()) if not frame_data.empty else 0
                            
                            frames.append(go.Frame(
                                data=[
                                    go.Scatter(x=track_df['x'], y=track_df['y']), 
                                    go.Scatter(
                                        x=frame_data['x'], 
                                        y=frame_data['y'],
                                        text=frame_data['driver_acronym'],
                                        ids=frame_data['driver_acronym'],
                                        textfont=dict(size=11, color=['red' if drv in [selected_driver, 'GHOST'] else 'white' for drv in frame_data['driver_acronym']], weight='bold'),
                                        marker=dict(color=frame_data['team_colour'], size=12)
                                    ),
                                    go.Scatter(
                                        x=[hud_x],
                                        y=[hud_y],
                                        text=[f"Lap {curr_lap}"]
                                    )
                                ],
                                name=str(t)
                            ))
                        
                        replay_fig.frames = frames

                        # Configure animation settings, including layout and controls for play, pause, and speed adjustment, allowing users to interact with the replay and analyze the predicted strategy in detail.
                        replay_fig.update_layout(
                            height=1000,
                            plot_bgcolor='rgba(0,0,0,0)',
                            paper_bgcolor='rgba(0,0,0,0)',
                            xaxis=dict(range=[x_min, x_max], visible=False, fixedrange=True),
                            yaxis=dict(range=[y_min, y_max], visible=False, fixedrange=True, scaleanchor="x", scaleratio=1),
                            showlegend=False,
                            margin=dict(t=20, l=20, r=20, b=20),
                            updatemenus=[dict(
                                type="buttons",
                                showactive=True,
                                x=0.5, y=-0.05,
                                xanchor="center", yanchor="top",
                                direction="left",
                                buttons=[
                                    dict(label="Restart", method="animate", args=[[str(timestamps_to_animate[0])], dict(frame=dict(duration=0, redraw=True), mode="immediate")]),
                                    dict(label="Slow", method="animate", args=[None, dict(frame=dict(duration=400, redraw=False), transition=dict(duration=400))]),
                                    dict(label="Play", method="animate", args=[None, dict(frame=dict(duration=200, redraw=False), transition=dict(duration=200))]),
                                    dict(label="Fast", method="animate", args=[None, dict(frame=dict(duration=80, redraw=False), transition=dict(duration=80))]),
                                    dict(label="Pause", method="animate", args=[[None], dict(frame=dict(duration=0, redraw=False), mode="immediate")])
                                ],
                                bgcolor="rgba(0,0,0,0.5)",
                                font=dict(color="white", size=14)
                            )]
                        )
                        
                        st.session_state[replay_fig_key] = replay_fig
                    else:
                        st.warning("No data available for animation.")
                        replay_fig = None

                    if replay_fig:
                        st.markdown("<div class='glass-card'>", unsafe_allow_html=True)
                        st.plotly_chart(st.session_state[replay_fig_key], width='stretch', config={"displayModeBar": False})
                        st.markdown("</div>", unsafe_allow_html=True)
                        
                with right_col:
                    # Position over laps for historic and predicted strategy
                    st.markdown("<div style='text-align:center; font-size:16px; font-weight:600; margin-top:16px; margin-bottom:4px'>Position Over Laps</div>", unsafe_allow_html=True)
                    
                    history_list = []
                    laps_to_process = sorted(all_data['lap_number'].unique())
                    field_drivers = all_data[~all_data['driver_acronym'].isin([selected_driver, 'GHOST'])]
                    
                    # Calculate positions for each lap
                    for lap in laps_to_process:
                        if lap < start_lap or lap > max_lap_number - 1: # We only calculate positions for laps within the range of the simulation
                            continue
                            
                        
                        field_lap_data = field_drivers[field_drivers['lap_number'] == lap]
                        field_times = field_lap_data.groupby('driver_acronym')['race_time'].max()
                        
                        # Append historic lap, driver and position based on rankings for each lap by race time
                        real_lap_data = all_data[(all_data['driver_acronym'] == selected_driver) & (all_data['lap_number'] == lap)]
                        if not real_lap_data.empty:
                            real_time = real_lap_data['race_time'].max()
                            real_pos = 1 + (field_times < real_time).sum()
                            history_list.append({
                                'Lap': lap,
                                'Driver': selected_driver,
                                'Pos': int(real_pos)
                            })
                            
                        # Append predicted lap, driver and position based on rankings for each lap by race time
                        ghost_lap_data = all_data[(all_data['driver_acronym'] == 'GHOST') & (all_data['lap_number'] == lap)]
                        if not ghost_lap_data.empty:
                            ghost_time = ghost_lap_data['race_time'].max()
                            ghost_pos = 1 + (field_times < ghost_time).sum()
                            history_list.append({
                                'Lap': lap,
                                'Driver': 'GHOST',
                                'Pos': int(ghost_pos)
                            })

                    result = pd.DataFrame(history_list)
                    
                    # Trace position data
                    if not result.empty:
                        pos_fig = go.Figure()
                        colors = {selected_driver: '#1f77b4', 'GHOST': '#ff7f0e'}

                        for driver_name in result['Driver'].unique():
                            driver_data = result[result['Driver'] == driver_name]
                            pos_fig.add_trace(go.Scatter(
                                x=driver_data['Lap'],
                                y=driver_data['Pos'],
                                mode='lines+markers',
                                name=driver_name,
                                line=dict(width=3, color=colors.get(driver_name, '#999')),
                                marker=dict(size=8),
                                hovertemplate="Lap %{x}<br>Pos: %{y}<extra></extra>"
                            ))

                        pos_fig.update_layout(
                            title=f"Projected Finish vs Reality",
                            xaxis_title="Lap Number",
                            yaxis_title="Position",
                            yaxis_autorange='reversed',
                            yaxis=dict(tickmode='linear', dtick=1),
                            template="plotly_white",
                            height=400,
                            showlegend=True
                        )
                        st.plotly_chart(pos_fig, width='stretch')
                    else:
                        st.info("Not enough data to calculate position history.")

# Run the simulation when the page is loaded, using the session key to fetch the relevant data and generate the visualizations
start_simulation(session_key)