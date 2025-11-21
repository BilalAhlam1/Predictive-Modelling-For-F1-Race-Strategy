import asyncio
import streamlit as st
import pandas as pd
import numpy as np
import time
import sys, os; sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'DataCollection')))
import storeRaceData as raceData
import openf1_helper as of1
api = of1.api


# --- RACE DATA CONFIGURATION ---
with st.spinner('Loading recent races... please wait.'):
    fetched = raceData.fetch_last_five_sessions()
if not fetched:
    st.error("Failed to load recent races. Please try again later.")
    st.stop()
else:
    st.success("Recent races loaded successfully.")

st.success("Done!")

st.write(
    """
    # Predictive Modelling For F1 Race Strategy Dashboard

    Welcome to the F1 Race Strategy dashboard! This dashboard provides tools and visualizations to help teams and analysts make informed decisions during races. We leverage historical race data and machine learning models to predict optimal pit stop strategies
    and tire choices based on real-time race conditions. You can explore various scenarios, analyze driver performance, and visualize race outcomes based on different strategies as well as historical data.
    """
)

st.info(
    """
    This project is entirely unofficial
    and is not affiliated with, endorsed by, or associated in any way with Formula 1 companies,
    the FIA, or any official Formula 1 entities. All references to F1, FORMULA ONE,
    FORMULA 1, FIA FORMULA ONE WORLD CHAMPIONSHIP, GRAND PRIX, and
    related trademarks are the property of Formula One Licensing B.V. and are used solely for
    academic and educational purposes. Similarly, any APIs, data sources, or logos referenced
    from OpenF1 or FastF1 are acknowledged as third-party intellectual property and are used in
    accordance with their respective terms and licenses.
    """,
    icon="ℹ️",
)