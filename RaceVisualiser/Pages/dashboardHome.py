import streamlit as st
import matplotlib.pyplot as plt
import pandas as pd
import time
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'DataCollection')))
import storeRaceData as raceData

# --- REFRESH CACHE --- #
# Clear Streamlit caches once when the Dashboard page is first opened.
# This forces fresh data for the whole app on first visit to this page, and maintains performance afterwards (when revisiting).
st.cache_data.clear()        # clear @st.cache_data decorated results
st.cache_resource.clear()    # clear @st.cache_resource decorated results


# ---- GLOBAL THEME ----
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
            padding-top: 3rem !important;
        }

        /* Hero */
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
            margin-top: 16px;
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

        /* Info Banner */
        .info-banner {
            background: rgba(122, 162, 255, 0.08);
            border: 1px solid rgba(122, 162, 255, 0.2);
            border-radius: 16px;
            padding: 14px 18px;
            margin-bottom: 20px;
            color: var(--muted);
            font-size: 13px;
        }

        /* Race Cards */
        div[data-testid="stVerticalBlock"] > div { gap: 1rem; }

        .race-card-header {
            font-size: 13px;
            font-weight: 600;
            color: var(--muted);
            margin-bottom: 6px;
            letter-spacing: 0.02em;
        }

        .race-title {
            font-size: 20px;
            font-weight: 700;
            margin-bottom: 4px;
            color: #f8fafc;
        }

        .race-date {
            font-size: 12px;
            color: var(--muted);
            margin-bottom: 12px;
        }

        .race-type {
            font-size: 11px;
            color: var(--accent);
            font-weight: 600;
            margin-bottom: 4px;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }

        /* Buttons */
        .stButton > button {
            border-radius: 12px;
            font-weight: 600;
            letter-spacing: 0.02em;
            transition: all 0.2s ease;
        }

        .stButton > button:hover {
            transform: translateY(-2px);
        }

        /* Section Titles */
        .section-title {
            font-size: 22px;
            font-weight: 700;
            color: #f8fafc;
            margin: 24px 0 16px 0;
            letter-spacing: -0.01em;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

# --- Getter for Track Map Image --- #
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

# --- Country to Flag Emoji --- #
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

# --- HERO & DESCRIPTION SECTION --- #
st.markdown(
    """
    <div class="hero-shell">
        <div class="hero-pill">F1 Strategy Dashboard</div>
        <div>
            <div class="hero-title">Predictive Modelling For Race Strategy</div>
            <div class="hero-subtext">Analyze telemetry, visualize replays, and explore race data</div>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="info-banner">
        ℹ️ This project is entirely unofficial and is not affiliated with, endorsed by, or associated in any way with Formula 1 companies,
        the FIA, or any official Formula 1 entities. All references to F1 and related trademarks are the property of Formula One Licensing B.V.
    </div>
    """,
    unsafe_allow_html=True,
)

# --- RACE CALENDAR / SELECTION SECTION --- #
session_df = raceData.tableOfRaces()

if session_df.empty:
    st.error("No race data available.")
else:
    session_df['date_start'] = pd.to_datetime(session_df['date_start'])
    st.markdown(f"<div class='section-title'>{raceData.get_season_year()} Race Calendar</div>", unsafe_allow_html=True)
    session_df = session_df.sort_values(by='date_start', ascending=False) # Most recent first
    cols_per_row = 3
    rows = [session_df.iloc[i:i + cols_per_row] for i in range(0, len(session_df), cols_per_row)]
    
    for row_chunk in rows:
        cols = st.columns(cols_per_row)
        for idx, (_, race) in enumerate(row_chunk.iterrows()):
            with cols[idx]:
                
                is_selected = st.session_state.get('selected_session_key') == race['session_key']
                
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
                
                st.markdown("</div>", unsafe_allow_html=True)

# --- DEBUG / CONFIRMATION --- #
if 'selected_session_key' in st.session_state:
    st.success(f"**{st.session_state['selected_race_name']}** selected. Go to 'Race Replay' page to view analysis.")