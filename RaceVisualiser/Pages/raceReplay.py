import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'DataCollection')))
import storeRaceData as raceData

st.header("Race Replay & Telemetry")

# Check if user selected a race
if 'selected_session_key' not in st.session_state:
    st.warning("No race selected.")
    st.stop()

session_key = st.session_state['selected_session_key']
race_name = st.session_state.get('selected_race_name', 'Unknown GP')

st.write(f"### Analyzing: {race_name}")
st.caption(f"Session Key: {session_key}")

# --- GET_STATIC_TRACK ---
@st.cache_data
def get_static_track(key):
    return raceData.get_track_layout(key)

# --- ANIMATE_RACE ---
def play_race_replay(session_key):
    
    # Load Dynamic Driver Data
    df = raceData.get_race_replay_data(session_key)
    # Load Static Track Data
    track_df = get_static_track(session_key)

    if df.empty:
        st.error("No race replay data available.")
        return

    
    if track_df is None or track_df.empty:
        st.error("Could not load track layout.")
        return

    # Define Axis Ranges For Consistent View which keeps the track centered and padded from the frame edges
    padding = 500
    x_min, x_max = track_df['x'].min() - padding, track_df['x'].max() + padding
    y_min, y_max = track_df['y'].min() - padding, track_df['y'].max() + padding

    # Process Timestamps
    all_timestamps = sorted(df['race_time'].unique())
    animation_timestamps = all_timestamps[::2] # Animation frames that skip every other timestamp for performance

    # Generate Animation Frames
    frames = []
    for t in animation_timestamps: 
        frame_data = df[df['race_time'] == t]
        
        # Determine max lap for label
        curr_lap = int(frame_data['lap_number'].max()) if not frame_data.empty else 0

        frames.append(go.Frame(
            data=[go.Scatter(
                x=frame_data['x'],
                y=frame_data['y'],
                # This links points by Name, not by list index.
                # Prevents issue with drivers floating when the list of drivers changes.
                ids=frame_data['driver_acronym'],
                mode='markers+text',
                text=frame_data['driver_acronym'],
                textposition="top center",
                textfont=dict(size=10, color="white"),
                marker=dict(color='red', size=10, line=dict(width=1, color='white'))
            )],
            name=str(t), 
            traces=[1] 
        ))

    # Generate Slider Steps (Laps)
    # Filter to only times that exist in our animation_timestamps
    df_anim = df[df['race_time'].isin(animation_timestamps)]
    lap_start_times = df_anim.groupby('lap_number')['race_time'].min().sort_index()
    
    slider_steps = []
    for lap, start_time in lap_start_times.items():
        slider_steps.append(dict(
            method="animate",
            args=[[str(start_time)], dict(frame=dict(duration=0, redraw=False), mode="immediate")],
            label=str(lap)
        ))

    # Build Figure
    # Get initial data for the very first frame
    start_data = df[df['race_time'] == animation_timestamps[0]]

    fig = go.Figure(
        data=[
            # Trace Static Track
            go.Scatter(
                x=track_df['x'],
                y=track_df['y'],
                mode='lines',
                line=dict(color='#444', width=4),
                hoverinfo='skip',
                name='Track'
            ),
            # Trace Initial Drivers
            go.Scatter(
                x=start_data['x'],
                y=start_data['y'],
                ids=start_data['driver_acronym'], # link points by Name
                mode='markers',
                marker=dict(color='red', size=10),
                name='Drivers'
            )
        ],
        frames=frames
    )

    # Layout
    fig.update_layout(
        height=700,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        xaxis=dict(range=[x_min, x_max], visible=False, fixedrange=True),
        yaxis=dict(range=[y_min, y_max], visible=False, fixedrange=True, scaleanchor="x", scaleratio=1),
        showlegend=False,
        title="Race Replay",
        
        updatemenus=[dict(
            type="buttons",
            showactive=False,
            x=0.1, y=0,
            buttons=[
                dict(label="Play",
                     method="animate",
                     args=[None, dict(frame=dict(duration=100, redraw=False), fromcurrent=True)]),
                dict(label="Pause",
                     method="animate",
                     args=[[None], dict(frame=dict(duration=0, redraw=False), mode="immediate")])
            ]
        )],
        
        sliders=[dict(
            currentvalue={
                "visible": True, 
                "prefix": "Start of Lap: ", 
                "xanchor": "right",
                "font": {"size": 20, "color": "white"}
            },
            pad={"t": 50, "b": 10},
            steps=slider_steps 
        )]
    )

    st.plotly_chart(fig, use_container_width=True)

# --- RUN ---
play_race_replay(session_key)