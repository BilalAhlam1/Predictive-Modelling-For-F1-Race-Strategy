import streamlit as st
import time
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'DataCollection')))
import storeRaceData as raceData

# --- PAGE CONFIG ---
st.set_page_config(layout="wide", page_title="F1 Strategy Dashboard")

# --- DATA LOADING PHASE ---
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
        st.error("Failed to load recent races. Please check connection.")
        st.stop()
    else:
        st.session_state["data_loaded"] = True
        time.sleep(0.5) 
        st.rerun() # Reload to show the Navigation Bar

# --- NAVIGATION PHASE ---
# This only runs after data is loaded and sidebar is allowed to show

# Define the pages
home_page = st.Page("Pages/dashboardHome.py", title="Home", icon="🏠", default=True)
replay_page = st.Page("Pages/raceReplay.py", title="Race Replay", icon="🏎️")

# Create the Navigation Object
pg = st.navigation({
    "Dashboard": [home_page],
    "Analysis": [replay_page]
})

# Run the selected page
pg.run()