import subprocess
import streamlit as st
import time
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'DataCollection')))
import storeRaceData as raceData

# --- DATABASE INITIALIZATION ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "DatabaseConnection", "f1_strategy.db")
INIT_SCRIPT = os.path.join(BASE_DIR, "DatabaseConnection", "createDatabase.py")

def initialize_database():
    """
    Checks if the local SQLite database exists.
    If not, runs the initialization script to create tables and populate with historical F1 data.
    """
    if not os.path.exists(DB_PATH):
        st.warning("Database not found. Initializing local storage...")
        try:
            with st.spinner("Fetching historical F1 data and building local cache..."):
                subprocess.run(
                    [sys.executable, INIT_SCRIPT],
                    capture_output=True,
                    text=True,
                    check=True,
                    cwd=os.path.dirname(INIT_SCRIPT)
                )
            st.success("Database initialized successfully!")
        except subprocess.CalledProcessError as e:
            st.error(f"Database initialization failed: {e.stderr}")
            st.stop()

initialize_database()

# --- PAGE CONFIGURATION ---
st.set_page_config(layout="wide", page_title="F1 Strategy Dashboard")

# --- INITIAL DATA LOADING ---
# On first load, fetch the latest race data and hide UI elements during loading
if "data_loaded" not in st.session_state:
    st.markdown(
        """
        <style>
            [data-testid="stSidebar"] {display: none;}
            [data-testid="stHeader"] {visibility: hidden;}
            [data-testid="collapsedControl"] {display: none;}
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

    with st.spinner('Initializing Dashboard & Syncing Latest Races...'):
        data_fetch_success = raceData.update_last_five_sessions()
    
    if not data_fetch_success:
        st.error("Failed to load race data. A live race may be in progress. Please try again later.")
        st.stop()
    
    st.session_state["data_loaded"] = True
    time.sleep(0.5)
    st.rerun()

# --- NAVIGATION ---
# Define dashboard pages
home_page = st.Page("Pages/dashboardHome.py", title="Home", icon="🏠", default=True)
replay_page = st.Page("Pages/raceReplay.py", title="Race Replay", icon="🏎️")
prediction_page = st.Page("Pages/strategyPrediction.py", title="Strategy Prediction", icon="📊")

# Create navigation menu
pg = st.navigation({
    "Dashboard": [home_page],
    "Analysis": [replay_page],
    "Prediction Model": [prediction_page]
})

# Render the selected page
pg.run()