import streamlit as st
import pandas as pd
import sys
import os

# Add path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'DataCollection')))
import storeRaceData as raceData

st.header("Race Replay & Telemetry")

# Check if user selected a race on the Home Page
if 'selected_session_key' not in st.session_state:
    st.warning("No race selected.")
    st.info("Please go to the **Home** page and select a race from the calendar first.")
    st.stop()

# Load Data for the selected session
session_key = st.session_state['selected_session_key']
race_name = st.session_state.get('selected_race_name', 'Unknown GP')

st.write(f"### Analyzing: {race_name}")
st.caption(f"Session Key: {session_key}")

# --- GRAPHING ---