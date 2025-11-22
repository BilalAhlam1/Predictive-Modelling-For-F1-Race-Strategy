import asyncio
import streamlit as st
import pandas as pd
import numpy as np
import time
import sys, os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'DataCollection')))
import storeRaceData as raceData
import openf1_helper as of1
api = of1.api

# --- PAGE SETUP ---
st.set_page_config(layout="wide", page_title="F1 Strategy Dashboard")

# --- CUSTOM CSS ---
# This CSS removes some default padding to make the cards look tighter and cleaner
st.markdown("""
    <style>
    div[data-testid="stVerticalBlock"] > div {
        gap: 1rem;
    }
    .race-card-header {
        font-size: 14px;
        font-weight: bold;
        color: #666;
        margin-bottom: 5px;
    }
    .race-title {
        font-size: 18px;
        font-weight: 800;
        margin-bottom: 5px;
    }
    .race-date {
        font-size: 12px;
        color: #888;
        margin-bottom: 10px;
    }
    </style>
""", unsafe_allow_html=True)

# --- Country to Flag Emoji ---
# Simple mapping for common F1 countries to match the visual style
def get_flag(country_name):
    flags = {
        "Bahrain": "🇧🇭", "Saudi Arabia": "🇸🇦", "Australia": "🇦🇺", "Japan": "🇯🇵",
        "China": "🇨🇳", "Miami": "🇺🇸", "USA": "🇺🇸", "United States": "🇺🇸",
        "Italy": "🇮🇹", "Monaco": "🇲🇨", "Canada": "🇨🇦", "Spain": "🇪🇸",
        "Austria": "🇦🇹", "Great Britain": "🇬🇧", "UK": "🇬🇧", "Hungary": "🇭🇺",
        "Belgium": "🇧🇪", "Netherlands": "🇳🇱", "Azerbaijan": "🇦🇿", "Singapore": "🇸🇬",
        "Mexico": "🇲🇽", "Brazil": "🇧🇷", "Las Vegas": "🇺🇸", "Qatar": "🇶🇦", "Abu Dhabi": "🇦🇪"
    }
    return flags.get(country_name, "🏁") # Default flag if not found

# --- MAIN ---

# Fetch Data
session_df = raceData.tableOfRaces()

if session_df.empty:
    st.error("No race data available.")
else:
    # Ensure date is datetime for sorting/formatting
    session_df['date_start'] = pd.to_datetime(session_df['date_start'])
    
    # Page Title
    st.write(f"## {time.localtime().tm_year} Race Calendar")
    
    # Grid Layout Logic
    # 3 cards per row
    cols_per_row = 3
    rows = [session_df.iloc[i:i + cols_per_row] for i in range(0, len(session_df), cols_per_row)]

    for row_chunk in rows:
        cols = st.columns(cols_per_row)
        
        # Iterate through the chunk of races for this row
        for idx, (_, race) in enumerate(row_chunk.iterrows()):
            with cols[idx]:
                
                # styling 
                is_selected = st.session_state.get('selected_session_key') == race['session_key']
                border_color = "red" if is_selected else None 
                
                # Create the Card container
                with st.container(border=True):
                    # -- Header--
                    flag = get_flag(race['country_name'])
                    st.markdown(f"<div class='race-card-header'>{flag} {race['country_name'].upper()}</div>", unsafe_allow_html=True)

                    # -- Race Type --
                    st.markdown(f"<div class='race-Type'>{race['session_name']}</div>", unsafe_allow_html=True)

                    # -- Title --
                    st.markdown(f"<div class='race-title'>{race['location']}</div>", unsafe_allow_html=True)
                    
                    # -- Date --
                    date_str = race['date_start'].strftime("%d %b %Y")
                    st.markdown(f"<div class='race-date'>{date_str}</div>", unsafe_allow_html=True)
                    
                    # -- Action Button --
                    # The button key must be unique, so we use the session_key
                    if st.button("Select Race", key=f"btn_{race['session_key']}", use_container_width=True, type="primary" if is_selected else "secondary"):
                        # --- pass session key as parameter ---
                        st.session_state['selected_session_key'] = race['session_key']
                        st.session_state['selected_race_name'] = race['location']
                        st.rerun() # Rerun to update the dashboard with new data

# --- DEBUG ---
if 'selected_session_key' in st.session_state:
    st.divider()
    st.success(f"Currently Analyzing: **{st.session_state['selected_race_name']}** (Key: {st.session_state['selected_session_key']})")
    # pass session key to data visualiser