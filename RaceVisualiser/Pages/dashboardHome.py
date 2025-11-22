import streamlit as st
import matplotlib.pyplot as plt
import pandas as pd
import time
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'DataCollection')))
import storeRaceData as raceData

# --- CSS FOR CARDS ---
st.markdown("""
    <style>
    div[data-testid="stVerticalBlock"] > div { gap: 1rem; }
    .race-card-header { font-size: 14px; font-weight: bold; color: #666; margin-bottom: 5px; }
    .race-title { font-size: 18px; font-weight: 800; margin-bottom: 5px; }
    .race-date { font-size: 12px; color: #888; margin-bottom: 10px; }
    .race-type { font-size: 12px; color: #FF1801; font-weight: bold; margin-bottom: 2px; }
    </style>
""", unsafe_allow_html=True)

# --- Getter for Track Map Image ---
@st.cache_data(show_spinner=False)
def get_track_map_image(session_key):
    """
    Wrapper to fetch and plot track map.
    Cached so we don't re-query the DB on every button click.
    """
    try:
        df = raceData.get_track_layout(session_key)
        fig = raceData.plot_track_map(df)
        return fig
    except Exception:
        return None

# --- Country to Flag Emoji ---
def get_flag(country_name):
    flags = {
        "Bahrain": "🇧🇭", "Saudi Arabia": "🇸🇦", "Australia": "🇦🇺", "Japan": "🇯🇵",
        "China": "🇨🇳", "Miami": "🇺🇸", "USA": "🇺🇸", "United States": "🇺🇸",
        "Italy": "🇮🇹", "Monaco": "🇲🇨", "Canada": "🇨🇦", "Spain": "🇪🇸",
        "Austria": "🇦🇹", "Great Britain": "🇬🇧", "UK": "🇬🇧", "Hungary": "🇭🇺",
        "Belgium": "🇧🇪", "Netherlands": "🇳🇱", "Azerbaijan": "🇦🇿", "Singapore": "🇸🇬",
        "Mexico": "🇲🇽", "Brazil": "🇧🇷", "Las Vegas": "🇺🇸", "Qatar": "🇶🇦", "Abu Dhabi": "🇦🇪"
    }
    return flags.get(country_name, "🏁")

# --- ETHICS & DESCRIPTION SECTION ---
st.write(
    """
    # Predictive Modelling For F1 Race Strategy Dashboard
    Welcome to the F1 Race Strategy dashboard! This dashboard provides tools and visualizations to help teams and analysts make informed decisions during races. 
    Select a race from the calendar below to begin your analysis.
    """
)

st.info(
    """
    This project is entirely unofficial and is not affiliated with, endorsed by, or associated in any way with Formula 1 companies,
    the FIA, or any official Formula 1 entities. All references to F1 and related trademarks are the property of Formula One Licensing B.V.
    """,
    icon="ℹ️",
)

st.divider()

# --- RACE CALENDAR / SELECTION SECTION ---
session_df = raceData.tableOfRaces()

if session_df.empty:
    st.error("No race data available.")
else:
    session_df['date_start'] = pd.to_datetime(session_df['date_start'])
    
    st.subheader(f"{time.localtime().tm_year} Race Calendar")
    
    cols_per_row = 3
    rows = [session_df.iloc[i:i + cols_per_row] for i in range(0, len(session_df), cols_per_row)]

    for row_chunk in rows:
        cols = st.columns(cols_per_row)
        for idx, (_, race) in enumerate(row_chunk.iterrows()):
            with cols[idx]:
                
                is_selected = st.session_state.get('selected_session_key') == race['session_key']
                
                with st.container(border=True):
                    # Flag & Country
                    flag = get_flag(race['country_name'])
                    st.markdown(f"<div class='race-card-header'>{flag} {race['country_name'].upper()}</div>", unsafe_allow_html=True)

                    # Race Type (Race vs Sprint)
                    r_name = race.get('session_name', 'Race') 
                    st.markdown(f"<div class='race-type'>{r_name}</div>", unsafe_allow_html=True)

                    # Location
                    st.markdown(f"<div class='race-title'>{race['location']}</div>", unsafe_allow_html=True)
                    
                    # Date
                    date_str = race['date_start'].strftime("%d %b %Y")
                    st.markdown(f"<div class='race-date'>{date_str}</div>", unsafe_allow_html=True)

                    # Track Map
                    track_fig = get_track_map_image(race['session_key'])
                    if track_fig:
                        # Display the plot
                        st.pyplot(track_fig, use_container_width=True, clear_figure=True)
                    else:
                        # Fallback space if no data
                        st.markdown("<br><br>", unsafe_allow_html=True)
                    
                    # Action Button
                    if st.button("Select Race", key=f"btn_{race['session_key']}", use_container_width=True, type="primary" if is_selected else "secondary"):
                        st.session_state['selected_session_key'] = race['session_key']
                        st.session_state['selected_race_name'] = race['location']
                        st.rerun()

# --- DEBUG / CONFIRMATION ---
if 'selected_session_key' in st.session_state:
    st.success(f"**{st.session_state['selected_race_name']}** selected. Go to 'Race Replay' page to view analysis.")