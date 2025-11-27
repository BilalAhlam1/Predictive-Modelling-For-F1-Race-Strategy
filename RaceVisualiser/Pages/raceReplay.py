import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'DataCollection')))
import storeRaceData as raceData

st.header("Race Replay & Telemetry")

if 'selected_session_key' not in st.session_state:
    st.warning("No race selected.")
    st.stop()

session_key = st.session_state['selected_session_key']
race_name = st.session_state.get('selected_race_name', 'Unknown GP')

# --- DATA LOADING PHASE ---
# If data hasn't been checked yet, show spinner and hide sidebar
if "replay_loaded" not in st.session_state:
    
    # Hide Sidebar, Header, and Center the Spinner
    st.markdown(
        """
        <style>
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
    
# --- DATA HELPERS ---
@st.cache_data
def get_static_track(key):
    return raceData.get_track_layout(key)

@st.cache_data
def get_replay_data(key):
    """
    Fetches and processes race replay data for visualization.
    """
    df = raceData.get_race_replay_data(key)
    if df.empty: return df
    
    # Get Driver Colors
    df_colors = raceData.get_driver_colors(key)
    if not df_colors.empty:
        df = pd.merge(df, df_colors, on='driver_acronym', how='left')
        # Fill any individual drivers that missed a color mapping
        df['team_colour'] = df['team_colour'].fillna('#FF1508')
    else:
        # Fallback if API completely failed
        df['team_colour'] = '#FF1508'
        
        
    # Setup Time
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    start_time = df['timestamp'].min()
    df['race_time'] = (df['timestamp'] - start_time).dt.total_seconds()
    df = df.sort_values('race_time')

    # Determine Lap Start Times
    lap_start_times = df.groupby(['driver_acronym', 'lap_number'])['race_time'].min().reset_index()
    lap_start_times.rename(columns={'race_time': 'lap_start_time'}, inplace=True)
    df = pd.merge(df, lap_start_times, on=['driver_acronym', 'lap_number'], how='left')

    # Create Master Timeline to synchronize all drivers (fixes inconsistent leaderboard issues)
    # Changed from 0.2 to 1.0 to synchronsie to 1 second intervals for performance
    min_t = df['race_time'].min() # Starting from first timestamp
    max_t = df['race_time'].max() # Ending at last timestamp
    master_timeline = np.arange(min_t, max_t, 1.0) 
    
    aligned_dfs = []
    
    # Align each driver's data to the 'master' timeline which has consistent 1.0s intervals for better animation
    for driver in df['driver_acronym'].unique():
        d_data = df[df['driver_acronym'] == driver].set_index('race_time')
        d_data = d_data[~d_data.index.duplicated(keep='first')]
        
        # Expand index to include master timeline and interpolate
        union_index = d_data.index.union(master_timeline)
        d_interp = d_data.reindex(union_index)
        
        # Carry over Driver Color
        d_color = d_data['team_colour'].iloc[0] if 'team_colour' in d_data.columns else '#FF1508'
        
        # Interpolate Coords Linearly
        d_interp['x'] = d_interp['x'].interpolate(method='slinear', limit_direction='both')
        d_interp['y'] = d_interp['y'].interpolate(method='slinear', limit_direction='both')
        
        # Fill Metadata
        d_interp = d_interp.ffill().bfill()
        
        # Filter to Master Timeline
        d_interp = d_interp.reindex(master_timeline)
        d_interp['driver_acronym'] = driver
        d_interp['team_colour'] = d_color
        
        aligned_dfs.append(d_interp.reset_index().rename(columns={'index': 'race_time'})) # Reset index for concatenation
        
    unified_df = pd.concat(aligned_dfs)
    unified_df['lap_number'] = unified_df['lap_number'].fillna(0).astype(int) # Fill missing laps as 0
    
    return unified_df

def get_leaderboard_for_frame(frame_data):
    """Generates leaderboard standings for a given frame based on lap number and lap start time."""
    if frame_data.empty:
        # Keep team columns so callers can rely on them existing
        return pd.DataFrame(columns=['Pos', 'Driver', 'Team', 'Lap', 'team_colour'])

    # Frame data may have multiple rows per driver (shouldn't normally), so take a single representative
    drivers = frame_data.groupby('driver_acronym', as_index=False).first()

    # Higher Lap First, Earlier Start Time Second
    standings = drivers.sort_values(by=['lap_number', 'lap_start_time'], ascending=[False, True]).copy()

    standings['Pos'] = range(1, len(standings) + 1)  # Assign Positions

    result = standings[['Pos', 'driver_acronym', 'lap_number', 'team_colour']].rename(
        columns={'driver_acronym': 'Driver', 'lap_number': 'Lap', 'team_colour': 'team_colour'})

    # Attach team name if present in the standings
    if 'team_name' in standings.columns:
        result['Team'] = standings['team_name'].fillna('')
    else:
        result['Team'] = ''

    # Reorder columns to Pos, Driver, Team, Lap, team_colour
    return result[['Pos', 'Driver', 'Team', 'Lap', 'team_colour']]

# --- Main Replay System ---
def play_race_replay(session_key):
    
    # Load Data
    with st.spinner(f"Optimizing {race_name} Data..."):
        df = get_replay_data(session_key)
        track_df = get_static_track(session_key)

        if df.empty or track_df is None:
            st.error("Data unavailable.")
            return
        st.session_state["replay_loaded"] = True
        # Setting up the Figure
        fig = make_subplots(
            rows=1, cols=2,
            column_widths=[0.90, 0.40], 
            specs=[[{"type": "xy"}, {"type": "table"}]],
            horizontal_spacing=0.02
        )

        # Define Axis Ranges
        padding = 400
        x_min, x_max = track_df['x'].min() - padding, track_df['x'].max() + padding
        y_min, y_max = track_df['y'].min() - padding, track_df['y'].max() + padding

        # Generate Frames
        # We use every single timestamp for smoothness
        animation_timestamps = df['race_time'].unique()
        
        frames = []
        for t in animation_timestamps:
            frame_data = df[df['race_time'] == t]
            lb_data = get_leaderboard_for_frame(frame_data)
            curr_lap = int(frame_data['lap_number'].max()) if not frame_data.empty else 0

            frames.append(go.Frame(
                data=[
                    # Trace Drivers
                    go.Scatter(
                        x=frame_data['x'] + 5, y=frame_data['y'],
                        ids=frame_data['driver_acronym'],
                        mode='markers+text',
                        text=frame_data['driver_acronym'],
                        textposition="top center",
                        cliponaxis=False, # Prevent text cutoff at edges
                        textfont=dict(size=11, color="white", weight="bold"),
                        marker=dict(color=frame_data['team_colour'], size=11, line=dict(width=1, color='white'))
                    ),
                    # Trace Table - include Team column with colour-coded text
                    go.Table(
                        header=dict(values=["Pos", "Driver", "Team", "Lap"], 
                                    fill_color='#111', 
                                    font=dict(color='white', size=13),
                                    height=20),
                        cells=dict(
                            values=[lb_data.Pos, lb_data.Driver, lb_data.Team, lb_data.Lap],
                            # background color for all cells
                            fill_color=[
                                ['#1e1e1e'] * len(lb_data),
                                ['#1e1e1e'] * len(lb_data),
                                ['#1e1e1e'] * len(lb_data),
                                ['#1e1e1e'] * len(lb_data)
                            ],
                            # colour the Team text using team_colour per-row, fallback to white
                            font=dict(
                                color=[
                                    ['white'] * len(lb_data),
                                    ['white'] * len(lb_data),
                                    lb_data['team_colour'].tolist() if 'team_colour' in lb_data.columns else ['white'] * len(lb_data),
                                    ['white'] * len(lb_data)
                                ],
                                size=13
                            ), 
                            height=20
                        )
                    )
                ],
                layout=go.Layout(title_text=f"Current Lap: {curr_lap}"),
                name=str(t),
                traces=[1, 2] # Table is trace 2 (index 1), Drivers is trace 1 (index 0)
            ))

        # Initial Traces
        start_t = animation_timestamps[0]
        start_data = df[df['race_time'] == start_t]
        start_lb = get_leaderboard_for_frame(start_data)

        # Trace Background Track
        fig.add_trace(go.Scatter(
            x=track_df['x'], y=track_df['y'],
            mode='lines', line=dict(color='#444', width=5), hoverinfo='skip'
        ), row=1, col=1)

        # Trace Drivers
        fig.add_trace(go.Scatter(
            x=start_data['x'], y=start_data['y'],
            mode='markers+text', text=start_data['driver_acronym'],
            textposition="top center",
            cliponaxis=False,
            textfont=dict(size=11, color="white", weight="bold"),
            marker=dict(color=start_data['team_colour'], size=10, line=dict(width=1, color='white'))
        ), row=1, col=1)

        # Trace Leaderboard Table
        fig.add_trace(go.Table(
            header=dict(values=["Pos", "Driver", "Team", "Lap"], fill_color='#111', font=dict(color='white')),
            cells=dict(
                values=[start_lb.Pos, start_lb.Driver, start_lb.Team, start_lb.Lap],
                # background color for all cells
                fill_color=[
                    ['#1e1e1e'] * len(start_lb),
                    ['#1e1e1e'] * len(start_lb),
                    ['#1e1e1e'] * len(start_lb),
                    ['#1e1e1e'] * len(start_lb)
                ],
                # colour the Team text using team_colour per-row, fallback to white
                font=dict(color=[
                    ['white'] * len(start_lb),
                    ['white'] * len(start_lb),
                    start_lb['team_colour'].tolist() if 'team_colour' in start_lb.columns else ['white'] * len(start_lb),
                    ['white'] * len(start_lb)
                ])
            )
        ), row=1, col=2)

        # Final Layout
        fig.update_layout(
            height=650,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            title=f"Race Start",
            xaxis=dict(range=[x_min, x_max], visible=False, fixedrange=True),
            yaxis=dict(range=[y_min, y_max], visible=False, fixedrange=True, scaleanchor="x", scaleratio=1),
            showlegend=False,

            updatemenus=[dict(
                type="buttons",
                showactive=True,
                x=0.05, y=-0.1,
                xanchor="left", yanchor="top",
                direction="left",
                buttons=[
                    dict(label="▶ Play",
                        method="animate",
                        args=[None, dict(
                            # Speed Optimization:
                            # 50ms per frame. 
                            # Since data is 1.0s interval, this will be 20x Real Time Speed.
                            # To be adjusted based on preference by user via multiplier later.
                            frame=dict(duration=80, redraw=True), 
                            transition=dict(duration=80, easing="linear"),
                            fromcurrent=True
                        )]),
                    dict(label="⏸ Pause",
                        method="animate",
                        args=[[None], dict(frame=dict(duration=0, redraw=False), mode="immediate")])
                ],
                bgcolor="white",
                bordercolor="#444",
                borderwidth=1,
                pad={"r": 10, "t": 10},
                font=dict(color="black")
            )],
            sliders=[] 
        )

    fig.frames = frames
    st.plotly_chart(fig, use_container_width=True) # Render the figure in Streamlit

play_race_replay(session_key)