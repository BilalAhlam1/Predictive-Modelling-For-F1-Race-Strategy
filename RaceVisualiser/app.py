import subprocess
import streamlit as st
import time
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'DataCollection')))
import storeRaceData as raceData

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "DatabaseConnection", "f1_strategy.db")
INIT_SCRIPT = os.path.join(BASE_DIR, "DatabaseConnection", "createDatabase.py")

def initialize_database():
    """Checks for the local SQLite database and runs initialization if missing."""
    if not os.path.exists(DB_PATH):
        st.warning("Local telemetry database not found. Initializing storage...")
        print("Database not found. Running initialization script...")
        try:
            # Execute the creation script using the current Python interpreter
            with st.spinner("Fetching historical F1 data and building local cache..."):
                result = subprocess.run(
                    [sys.executable, INIT_SCRIPT],
                    capture_output=True,
                    text=True,
                    check=True,
                    cwd=os.path.dirname(INIT_SCRIPT) # Run in its own directory to maintain relative paths
                )
            st.success("Database 'f1_strategy.db' created successfully!")
        except subprocess.CalledProcessError as e:
            st.error(f"Critical error during database initialization: {e.stderr}")
            st.stop()
    else:
        print("Database already exists. Skipping initialization.")
        pass

# Run the check before loading the rest of the dashboard
initialize_database()

# --- PAGE CONFIG --- #
st.set_page_config(layout="wide", page_title="F1 Strategy Dashboard")

# --- DATA LOADING PHASE --- #
# If data hasn't been checked yet, show spinner and hide sidebar
if "data_loaded" not in st.session_state:
    
    # Hide Sidebar, Header, and Center the Spinner
    st.markdown(
        """
        <style>
            [data-testid="stSidebar"] {display: none;}
            [data-testid="stHeader"] {visibility: hidden;}
            [data-testid="collapsedControl"] {display: none;}
            
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

    with st.spinner('Initializing Dashboard & Syncing Races...'):
        # This function should returns True if successful
        fetched = raceData.update_last_five_sessions()
    
    if not fetched:
        st.error("Failed to load races. There may be a live race in progress. Please try again later.") #API limitation issue
        st.stop()
    else:
        st.session_state["data_loaded"] = True
        time.sleep(0.5) 
        st.rerun() # Reload to show the Navigation Bar

# --- NAVIGATION PHASE --- #
# This only runs after data is loaded and sidebar is allowed to show

# Define the pages
home_page = st.Page("Pages/dashboardHome.py", title="Home", icon="🏠", default=True)
replay_page = st.Page("Pages/raceReplay.py", title="Race Replay", icon="🏎️")
prediction_strategy_page = st.Page("Pages/strategyPrediction.py", title="Strategy Prediction", icon="📊")

# Create the Navigation Object
pg = st.navigation({
    "Dashboard": [home_page],
    "Analysis": [replay_page],
    "Prediction Model": [prediction_strategy_page]
})

# Run the selected page
pg.run()