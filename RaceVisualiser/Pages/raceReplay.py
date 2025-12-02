import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'DataCollection')))
import storeRaceData as raceData
import storeMLData as mlData

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
                column_widths=[0.7, 0.4],
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

        # --------------- LINE GRAPH (LEFT SIDE) ---------------
        # Driver selector key for dropdown
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
            xaxis_title='Lap Number', yaxis_title='Lap Time (s)', height=500,
            plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)',
            legend=dict(
                orientation='v',
                y=1.02,
                yanchor='bottom',
                x=0.5,
                xanchor='center',
                traceorder='normal',
                font=dict(size=12)
            ),
            showlegend=False,
            margin=dict(t=5)
        )

        # Render lap figure in left column, then place the selectbox below it
        with left_col:
            # Build a custom vertical legend above the plot to ensure entries are stacked vertically
            # Title for the lap-time graph placed above the legend
            title_text = "Lap Times (All Drivers)" if selected_driver == "All" else f"Lap Times — {selected_driver}"
            st.markdown(f"<div style='text-align:center; font-size:16px; font-weight:600; margin-bottom:4px;'>{title_text}</div>", unsafe_allow_html=True)

            if selected_driver == "All" and drivers_list:
                # Build legend items as horizontal
                legend_items = []
                for drv in drivers_list:
                    try:
                        color = lap_times_df[lap_times_df['driver_acronym'] == drv]['team_colour'].iloc[0]
                    except Exception:
                        color = '#808080'
                    item_html = (
                        f"<div style='display:inline-flex; align-items:center; margin:4px 8px; font-size:12px;'>"
                        f"<span style='display:inline-block; width:14px; height:14px; background:{color}; border:1px solid #222;'></span>"
                        f"<span style='margin-left:6px; white-space:nowrap;'>{drv}</span>"
                        f"</div>"
                    )
                    legend_items.append(item_html)

                legend_html = "".join(legend_items)
                # Use flex container to lay items out horizontally and wrap to next line when needed
                st.markdown(
                    f"<div style='display:flex; flex-wrap:wrap; justify-content:center; gap:4px; max-width:100%; padding:2px 0;'>{legend_html}</div>",
                    unsafe_allow_html=True
                )

            st.plotly_chart(lap_fig, use_container_width=True, config={"displayModeBar": False})
            # This selectbox will update st.session_state[sel_key] and cause a rerun 
            # main_fig is cached in session_state so it won't rebuild keeping the race running
            st.selectbox("Driver (lap time graph)", options=["All"] + drivers_list, index=0 if selected_driver == "All" else (drivers_list.index(selected_driver) + 1), key=sel_key)
            
        # --------------- PIT INFO (RIGHT SIDE) ---------------
        
        with st.spinner("Loading Pit Stop Data..."):
            #Update last five sessions to get pit stop data
            fetch_data = mlData.update_last_five_sessions()
            if fetch_data:
                st.success("Pit stop data loaded successfully.")
            else:
                st.warning("Pit stop data unavailable; using API fallback.")
            
            #Fetch pit stop data for session
            pit_data = mlData.fetchMLData(session_key)
            if pit_data.empty:
                st.info("No pit stop data available for this race.")
            else:
                # ----------------- SHOW PIT STOP INFO -----------------
                # Define compound colors
                compound_colors = {
                    'SOFT': '#ff0000',
                    'MEDIUM': '#ffff00',
                    'HARD': '#ffffff',
                    'INTERMEDIATE': '#00ff00',
                    'WET': '#0099ff'
                }

                # Map driver_number from ML data to driver_acronym using telemetry driver mapping for y-axis
                try:
                    # telemetry contains `driver_number` from MLData and `driver_acronym` from RaceData which are combined as a map
                    if 'driver_number' in df.columns and 'driver_acronym' in df.columns:
                        driver_map = df[['driver_number', 'driver_acronym']].drop_duplicates()
                        # Ensure types align
                        pit_data['driver_number'] = pit_data['driver_number'].astype(driver_map['driver_number'].dtype)
                        pit_data = pit_data.merge(driver_map, on='driver_number', how='left')
                    else:
                        pit_data['driver_acronym'] = pit_data['driver_number'].astype(str)
                except Exception:
                    # use driver_number as string if mapping fails
                    pit_data['driver_acronym'] = pit_data['driver_number'].astype(str)

                # Build stints by merging contiguous laps with the same compound per driver
                pit_fig = go.Figure()

                # Normalize column names
                lap_col = 'lap_number' if 'lap_number' in pit_data.columns else ('lap' if 'lap' in pit_data.columns else ('pit_lap' if 'pit_lap' in pit_data.columns else None))
                compound_col = 'tire_compound' if 'tire_compound' in pit_data.columns else ('compound' if 'compound' in pit_data.columns else None)
                laps_on_tire_col = 'laps_on_tire' if 'laps_on_tire' in pit_data.columns else None

                # Build max lap per driver from lap_times_df to clip stints to race end
                max_lap_map = {}
                try:
                    if not lap_times_df.empty:
                        max_lap_series = lap_times_df.groupby('driver_acronym')['lap_number'].max() # Get max lap per driver
                        max_lap_map = max_lap_series.to_dict() # Convert to dict for easy lookup
                except Exception:
                    max_lap_map = {} # Fallback to empty if any issue

                y_order = pit_data['driver_acronym'].dropna().unique().tolist() # Preserve order of appearance

                # Build stints list
                stints = []
                for drv in y_order:
                    ddf = pit_data[pit_data['driver_acronym'] == drv].copy()
                    # If data contains a 'laps_on_tire' and a lap start, use that directly
                    if laps_on_tire_col and lap_col in ddf.columns:
                        ddf = ddf.sort_values(lap_col)
                        # For each row, build stint from lap and laps_on_tire
                        for _, r in ddf.iterrows():
                            start = int(r[lap_col])
                            length = int(r[laps_on_tire_col]) if pd.notna(r[laps_on_tire_col]) else 1
                            end = start + length - 1
                            stints.append({'driver': drv, 'start': start, 'end': end, 'compound': (str(r[compound_col]).upper() if compound_col in r and pd.notna(r[compound_col]) else '')})
                    else:
                        # Otherwise assume one row per lap with compound, merge contiguous runs
                        if lap_col not in ddf.columns or compound_col not in ddf.columns:
                            continue
                        ddf = ddf.sort_values(lap_col)
                        current_comp = None
                        current_start = None
                        prev_lap = None # Keep track of the previous lap
                        # Iterate through laps to build stints based on compound changes
                        for _, r in ddf.iterrows():
                            lap = int(r[lap_col])
                            comp = str(r[compound_col]).upper() if pd.notna(r[compound_col]) else ''
                            if current_comp is None:
                                current_comp = comp
                                current_start = lap
                            elif comp != current_comp: # Compound changed, end current stint
                                # Use the previously stored lap number for the end of the stint
                                stints.append({'driver': drv, 'start': current_start, 'end': prev_lap, 'compound': current_comp})
                                current_comp = comp
                                current_start = lap
                            prev_lap = lap # Update previous lap at the end of each iteration
                        if current_comp is not None:
                            stints.append({'driver': drv, 'start': current_start, 'end': ddf[lap_col].max(), 'compound': current_comp})

                # Add traces for each stint to the figure
                for s in stints:
                    drv = s.get('driver')
                    start = s.get('start')
                    end = s.get('end')
                    compound = (s.get('compound') or '').upper()

                    if start is None or end is None or drv is None:
                        continue

                    # Clip stint to driver's max lap if available to get stint end within race
                    # Reason: sometimes pit data may extend beyond race end due to data issues. Fixes overlapping stints.
                    max_l = max_lap_map.get(drv, end)
                    display_end = min(end, max_l)
                    
                    # Skip stints that end before they start (data issues)
                    if display_end < start:
                        continue

                    color = compound_colors.get(compound, '#808080') # Default grey if unknown
                    
                    # Add the main stint trace on top
                    pit_fig.add_trace(go.Scatter(
                        x=[start, display_end], # Laps on x-axis
                        y=[drv, drv], # Driver on y-axis
                        mode='lines',
                        line=dict(color=color, width=15),
                        name=compound,
                        showlegend=False,
                        hovertemplate=(f"Driver: {drv}<br>"
                                       f"Compound: {compound}<br>"
                                       f"Laps: {start}-{end}<br>"
                                       f"Stint Length: {end - start + 1}<extra></extra>")
                    ))
                  
                    
                # Add legend entries to explain colors
                for compound, color in compound_colors.items():
                    pit_fig.add_trace(go.Scatter(
                        x=[None], y=[None],
                        mode='markers',
                        marker=dict(size=10, color=color),
                        name=compound
                    ))

                # Set chart height based on number of drivers
                chart_height = max(600, len(y_order) * 30)

                # Final layout adjustments
                pit_fig.update_layout(
                    title=dict(text='Stints / Pit Tyres', x=0.5, xanchor='center'),
                    xaxis_title='Lap Number',
                    yaxis_title='Driver',
                    yaxis=dict(categoryorder='array', categoryarray=y_order[::-1]), # Show drivers top to bottom
                    height=chart_height,
                    template='plotly_dark',
                    showlegend=True
                )
                # Render pit figure in right column
                with right_col:
                    st.plotly_chart(pit_fig, use_container_width=True, config={"displayModeBar": False})
                    #cols_to_show = ['driver_number', 'driver_acronym'] + [c for c in pit_data.columns if c not in ['driver_number','driver_acronym']]
                    #st.dataframe(pit_data[cols_to_show].head(200))
                

# Start the Replay
play_race_replay(session_key)