import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'DataCollection')))
import storeRaceData as raceData

# --- RACE REPLAY SECTION ---
st.header(f"Race Replay & Telemetry For **{st.session_state['selected_race_name']}**")

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
    
#-----------------TRACK LAYOUT------------------#
@st.cache_data
def get_static_track(key):
    return raceData.get_track_layout(key)

#-----------------REPLAY DATA------------------#
@st.cache_data
def get_replay_data(key):
    """
    Fetches and processes race replay data for visualization.
    """
    # get_race_replay_data now returns (resampled_telemetry_df, lap_times_df)
    resampled, lap_times = raceData.get_race_replay_data(key)
    if resampled is None or (hasattr(resampled, 'empty') and resampled.empty):
        return pd.DataFrame(), pd.DataFrame()
    
    #-----------------DRIVER COLORS------------------#
    df = resampled
    df_colors = raceData.get_driver_colors(key)
    if not df_colors.empty:
        df = pd.merge(df, df_colors, on='driver_acronym', how='left')
        # Fill any individual drivers that missed a color mapping
        df['team_colour'] = df['team_colour'].fillna('#FF1508')
        
        # Merge colours into lap times as well so the line graph can use them
        lap_times = pd.merge(lap_times, df_colors[['driver_acronym','team_colour']], on='driver_acronym', how='left')
        lap_times['team_colour'] = lap_times['team_colour'].fillna('#FF1508')
    else:
        # Fallback if API completely failed
        df['team_colour'] = '#FF1508'
        lap_times['team_colour'] = '#FF1508'
    # Format lap times as mm:ss.mmm for display in hovertemplates for easier reading
    def _fmt_time_seconds(val):
        try:
            t = float(val)
        except Exception:
            return ''
        mins = int(t // 60)
        secs = int(t % 60)
        millis = int(round((t - int(t)) * 1000))
        return f"{mins}:{secs:02d}.{millis:03d}"

    if not lap_times.empty:
        lap_times['lap_time_fmt'] = lap_times['lap_time'].apply(_fmt_time_seconds)
    else:
        lap_times['lap_time_fmt'] = []
        
        
    #-----------------TIME SETUP------------------#
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
    
    return unified_df, lap_times


#-----------------LEADERBOARD------------------#
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
    with st.spinner(f"Optimizing {race_name} Data"):
        df, lap_times_df = get_replay_data(session_key)
        track_df = get_static_track(session_key)

        if df.empty or track_df is None:
            st.error("Data unavailable.")
            return
        try:
            if (not lap_times_df.empty) and (lap_times_df['team_colour'].nunique() == 1) and (lap_times_df['team_colour'].iloc[0] == '#FF1508'):
                st.warning("Driver colours unavailable (using defaults). OpenF1 API may be down; using DB fallback.") # API limitation issue, will fallback to default colors
        except Exception:
            # if lap_times_df doesn't have team_colour, skip warning
            pass
        
        # Mark as loaded for session loading position and avoid reloading
        st.session_state["replay_loaded"] = True

        # Build or reuse the heavy animated main figure
        main_fig_key = f"main_fig_{session_key}"
        if main_fig_key in st.session_state:
            main_fig = st.session_state[main_fig_key]
        else:
            #-----------------MAIN FIG SETUP (cached)------------------#
            main_fig = make_subplots(
                rows=1, cols=2,
                column_widths=[0.8, 0.2],
                specs=[[{"type": "xy"}, {"type": "table"}]],
                horizontal_spacing=0.02,
            )

            # Define Axis Ranges for main map
            padding = 400
            x_min, x_max = track_df['x'].min() - padding, track_df['x'].max() + padding
            y_min, y_max = track_df['y'].min() - padding, track_df['y'].max() + padding

            # Generate Frames for animation (drivers + leaderboard)
            animation_timestamps = df['race_time'].unique()
            frames = []
            for t in animation_timestamps:
                frame_data = df[df['race_time'] == t]
                lb_data = get_leaderboard_for_frame(frame_data)
                curr_lap = int(frame_data['lap_number'].max()) if not frame_data.empty else 0

                frames.append(go.Frame(
                    data=[
                                go.Scatter(
                                    x=frame_data['x'] + 5, y=frame_data['y'],
                                    ids=frame_data['driver_acronym'],
                                    mode='markers+text',
                                    text=frame_data['driver_acronym'],
                                    textposition="top center",
                                    cliponaxis=False,
                                    textfont=dict(size=13, color="white", weight="bold"),
                                    marker=dict(color=frame_data['team_colour'], size=16, line=dict(width=1, color='white'))
                                ),
                        go.Table(
                            header=dict(values=["Pos", "Driver", "Team", "Lap"], fill_color='#111', font=dict(color='white', size=16), height=30),
                            cells=dict(
                                values=[lb_data.Pos, lb_data.Driver, lb_data.Team, lb_data.Lap],
                                fill_color=[['#1e1e1e'] * len(lb_data)] * 4,
                                font=dict(
                                    color=[
                                        ['white'] * len(lb_data),
                                        ['white'] * len(lb_data),
                                        lb_data['team_colour'].tolist() if 'team_colour' in lb_data.columns else ['white'] * len(lb_data),
                                        ['white'] * len(lb_data)
                                    ],
                                    size=14
                                ),
                                height=28
                            )
                        )
                    ],
                    layout=go.Layout(title_text=f"Current Lap: {curr_lap}"),
                    name=str(t),
                    traces=[1, 2]
                ))

            # Initial traces track, drivers, table
            start_t = animation_timestamps[0]
            start_data = df[df['race_time'] == start_t]
            start_lb = get_leaderboard_for_frame(start_data)

            main_fig.add_trace(go.Scatter(x=track_df['x'], y=track_df['y'], mode='lines', line=dict(color='#444', width=8), hoverinfo='skip'), row=1, col=1)

            main_fig.add_trace(go.Scatter(
                x=start_data['x'], y=start_data['y'], mode='markers+text', text=start_data['driver_acronym'],
                textposition='top center', cliponaxis=False, textfont=dict(size=13, color='white', weight='bold'),
                marker=dict(color=start_data['team_colour'], size=14, line=dict(width=1, color='white'))
            ), row=1, col=1)

            main_fig.add_trace(go.Table(
                header=dict(values=["Pos", "Driver", "Team", "Lap"], fill_color='#111', font=dict(color='white', size=16), height=30),
                cells=dict(values=[start_lb.Pos, start_lb.Driver, start_lb.Team, start_lb.Lap],
                           fill_color=[['#1e1e1e'] * len(start_lb)] * 4,
                           font=dict(color=[['white'] * len(start_lb), ['white'] * len(start_lb), start_lb['team_colour'].tolist() if 'team_colour' in start_lb.columns else ['white'] * len(start_lb), ['white'] * len(start_lb)], size=14),
                           height=28)
            ), row=1, col=2)

            main_fig.frames = frames

            # play / pause buttons
            main_fig.update_layout(
                height=900,
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
                            args=[None, dict(frame=dict(duration=80, redraw=True), transition=dict(duration=80, easing="linear"), fromcurrent=True)]),
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

            # Cache main_fig in session_state so dropdowns don't rebuild it
            st.session_state[main_fig_key] = main_fig

        # Render main figure (track + leaderboard + play/pause buttons)
        st.plotly_chart(st.session_state[main_fig_key], use_container_width=True, config={"displayModeBar": False})

        # ----------------- LAP-TIME/PIT INFO GRAPH -----------------
        left_col, right_col = st.columns([0.5, 0.5]) # Split for lap time graph and pit info (pit info to be added later)

        # Driver selector key
        sel_key = f"lap_driver_{session_key}"
        drivers_list = []
        if not lap_times_df.empty:
            drivers_list = sorted(lap_times_df['driver_acronym'].unique())

        # Read selected driver from session_state
        selected_driver = st.session_state.get(sel_key, "All")

        # Build lap figure
        lap_fig = go.Figure()
        if not lap_times_df.empty:
            if selected_driver == "All": # Show all drivers
                for drv in drivers_list:
                    drv_df = lap_times_df[lap_times_df['driver_acronym'] == drv]
                    if drv_df.empty:
                        continue
                    lap_fig.add_trace(go.Scatter(
                        x=drv_df['lap_number'], y=drv_df['lap_time'], mode='lines+markers', name=drv,
                        line=dict(color=drv_df['team_colour'].iloc[0], width=2), marker=dict(size=6),
                        text=drv_df['lap_time_fmt'],
                        hovertemplate=f"<span style='font-size:14px'><b>{drv}: %{{text}}</b></span><br>Lap: %{{x}}<extra></extra>",
                        hoverlabel=dict(font=dict(size=14))
                    ))
            else: # Show selected driver only
                drv_df = lap_times_df[lap_times_df['driver_acronym'] == selected_driver]
                if not drv_df.empty:
                    lap_fig.add_trace(go.Scatter(
                        x=drv_df['lap_number'], y=drv_df['lap_time'], mode='lines+markers', name=selected_driver,
                        line=dict(color=drv_df['team_colour'].iloc[0], width=2), marker=dict(size=6),
                        text=drv_df['lap_time_fmt'],
                        hovertemplate=f"<span style='font-size:14px'><b>{selected_driver}: %{{text}}</b></span><br>Lap: %{{x}}<extra></extra>",
                        hoverlabel=dict(font=dict(size=14))
                    ))

        lap_fig.update_layout(
            xaxis_title='Lap Number', yaxis_title='Lap Time (s)', height=300,
            plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)'
        )

        # Render lap figure in left column, then place the selectbox below it
        with left_col:
            st.plotly_chart(lap_fig, use_container_width=True, config={"displayModeBar": False})
            # This selectbox will update st.session_state[sel_key] and cause a rerun 
            # main_fig is cached in session_state so it won't rebuild keeping the race running
            st.selectbox("Driver (lap time graph)", options=["All"] + drivers_list, index=0 if selected_driver == "All" else (drivers_list.index(selected_driver) + 1), key=sel_key)

# Start the Replay
play_race_replay(session_key)