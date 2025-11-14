import datetime
from matplotlib import animation
from matplotlib.patches import Circle
import numpy as np
import pandas as pd
import requests
import openf1_helper as of1
api = of1.api
from datetime import datetime, timedelta
import requests

from datetime import datetime, timedelta
import requests

import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from datetime import datetime
import time
import ast
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation

def plot_race(lap_locations):
    """
    Animate the entire race for a driver using coordinates from all laps.

    Args:
        lap_locations (dict): lap_number -> list of coordinates {'x','y','z','date'}
    """
    # Flatten all coordinates in order of lap and time
    all_coords = []
    lap_boundaries = []  # For coloring or marking lap changes
    for lap_number in sorted(lap_locations.keys()):
        coords = lap_locations[lap_number]
        if coords:
            all_coords.extend(coords)
            lap_boundaries.append(len(all_coords) - 1)

    if not all_coords:
        print("No coordinates to plot.")
        return

    x_vals = [c['x'] for c in all_coords]
    y_vals = [c['y'] for c in all_coords]

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(x_vals, y_vals, linestyle='--', color='gray', alpha=0.5)  # full track
    dot, = ax.plot([], [], 'ro', markersize=6)  # moving dot

    ax.set_title("Full Race Track Visualization")
    ax.set_xlabel("X")
    ax.set_ylabel("Y")
    ax.set_aspect('equal', adjustable='datalim')
    ax.grid(True)

    def init():
        dot.set_data([], [])
        return dot,

    def update(frame):
        dot.set_data([x_vals[frame]], [y_vals[frame]])
        return dot,

    # Interval controls animation speed (ms per frame)
    anim = FuncAnimation(fig, update, frames=len(x_vals),
                         init_func=init, blit=True, interval=50, repeat=False)

    plt.show()

def fetch_laps(session_key: int, driver_number: int):
    """Fetch all laps for a driver in a session, ignoring laps without date_start."""
    url = f"https://api.openf1.org/v1/laps?session_key={session_key}&driver_number={driver_number}"
    response = requests.get(url)
    response.raise_for_status()
    laps = response.json()

    # Filter out laps missing date_start
    laps = [lap for lap in laps if lap.get("date_start")]
    
    # Sort laps by start time
    laps.sort(key=lambda lap: lap["date_start"])
    return laps

def fetch_locations_for_lap(session_key: int, driver_number: int, start_time: str, end_time: str):
    """
    Fetch location coordinates for a driver for a given time range.
    """
    url = (
        f"https://api.openf1.org/v1/location?"
        f"session_key={session_key}&driver_number={driver_number}&"
        f"date>{start_time}&date<{end_time}"
    )
    response = requests.get(url)
    response.raise_for_status()
    locations = response.json()

    # Convert date strings to datetime objects
    for loc in locations:
        loc["date"] = datetime.fromisoformat(loc["date"])
    # Sort by datetime
    locations.sort(key=lambda loc: loc["date"])
    return locations

def map_locations_to_laps(session_key: int, driver_number: int):
    """Map coordinates to each lap using proper date ranges."""
    laps = fetch_laps(session_key, driver_number)
    lap_locations = {}  # lap_number -> list of coordinates

    for i, lap in enumerate(laps):
        lap_number = lap["lap_number"]
        start_time_str = lap.get("date_start")
        if not start_time_str:
            print(f"Warning: skipping lap {lap_number} with missing start_time")
            continue

        start_time = datetime.fromisoformat(start_time_str)
        lap_duration = lap.get("lap_duration") or 0  # Handle None safely
        if i + 1 < len(laps) and laps[i + 1].get("date_start"):
            end_time = datetime.fromisoformat(laps[i + 1]["date_start"])
        else:
            end_time = start_time + timedelta(seconds=lap_duration)

        # Convert datetimes to ISO strings for the API
        start_iso = start_time.isoformat()
        end_iso = end_time.isoformat()

        # Fetch location for this lap
        lap_coords = fetch_locations_for_lap(session_key, driver_number, start_iso, end_iso)
        time.sleep(0.2)  # brief pause to avoid rate limits
        lap_locations[lap_number] = lap_coords

    return lap_locations

def fetch_locations(session_key: int, driver_number: int):
    """Fetch all location points for a driver in a session."""
    url = f"https://api.openf1.org/v1/location?session_key={session_key}&driver_number={driver_number}"
    response = requests.get(url)
    response.raise_for_status()
    locations = response.json()
    # Convert date strings to datetime objects
    for loc in locations:
        loc["date"] = datetime.fromisoformat(loc["date"])
    # Sort by datetime
    locations.sort(key=lambda loc: loc["date"])
    return locations
    

def fetch_recent_f1_sessions():
    # Fetch sessions from the current year
    sessions_df = api.get_dataframe('sessions', {
        'year': datetime.now().year,
        'session_type': 'Race'
    })

    # Display last 5 races
    if not sessions_df.empty:
        sessions_df = sessions_df.sort_values('date_start')
        recent_sessions = sessions_df.tail(5)
        print("🏁 Recent F1 Races (2025):")
        print("=" * 60)
    for _, session in recent_sessions.iterrows():
        print(f"📍 {session['country_name']} GP - {session['location']}")
        print(f"   Session Key: {session['session_key']}")
        print(f"   Date: {session['date_start'][:10]}")
        print()
    
    # Select a session for analysis
    selected_session = recent_sessions.iloc[-1]  # Last race
    SESSION_KEY = selected_session['session_key']
    print(f"📊 Selected for analysis: {selected_session['country_name']} GP")
    print(f"   Session Key: {SESSION_KEY}")




CSV_FILE = "session_9869_lap_locations.csv"

def draw_race(csv_file):
    # Load CSV
    df = pd.read_csv(csv_file)
    
    # Convert timestamps to datetime objects
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Unique drivers
    drivers = df['driver_number'].unique()
    driver_map = dict(zip(df['driver_number'], df['driver_acronym']))
    
    # Store all positions per driver (sorted by timestamp)
    driver_positions = {}
    min_time, max_time = df['timestamp'].min(), df['timestamp'].max()
    
    for num in drivers:
        driver_df = df[df['driver_number'] == num].sort_values('timestamp')
        positions = list(zip(driver_df['timestamp'], driver_df['x'], driver_df['y'], driver_df['lap_number']))
        driver_positions[num] = positions
    
    # Build a common timeline (1-second intervals)
    timeline = pd.date_range(start=min_time, end=max_time, freq='1s')
    
    # Interpolate missing positions for each driver to align with timeline
    aligned_positions = {num: [] for num in drivers}
    for num in drivers:
        driver_pos = driver_positions[num]
        if not driver_pos:
            continue
        idx = 0
        last_x, last_y, last_lap = driver_pos[0][1], driver_pos[0][2], driver_pos[0][3]
        for t in timeline:
            while idx < len(driver_pos) and driver_pos[idx][0] <= t:
                last_x, last_y, last_lap = driver_pos[idx][1], driver_pos[idx][2], driver_pos[idx][3]
                idx += 1
            aligned_positions[num].append((last_x, last_y, last_lap))
    
    # Initialize plot
    fig, ax = plt.subplots(figsize=(12, 8))
    ax.set_title("F1 Race Replay")
    ax.set_xlabel("X")
    ax.set_ylabel("Y")
    
    # Draw approximate track using average of first driver positions
    first_driver = drivers[0]
    track_x = [pos[0] for pos in aligned_positions[first_driver]]
    track_y = [pos[1] for pos in aligned_positions[first_driver]]
    ax.plot(track_x, track_y, color='gray', linestyle='--', linewidth=1, label='Track')
    
    # Initialize driver markers and labels
    markers = {}
    labels = {}
    for num in drivers:
        x, y, _ = aligned_positions[num][0] if aligned_positions[num] else (0, 0, 0)
        markers[num], = ax.plot(x, y, 'o', label=driver_map[num])
        labels[num] = ax.text(x, y, driver_map[num], fontsize=9, color='black')
    
    lap_text = ax.text(0.95, 0.95, '', transform=ax.transAxes,
                       ha='right', va='top', fontsize=12,
                       bbox=dict(facecolor='white', alpha=0.7))
    
    # Animation update function
    def update(frame):
        frame_positions = []
        for num in drivers:
            if frame < len(aligned_positions[num]):
                x, y, lap = aligned_positions[num][frame]
            else:
                x, y, lap = aligned_positions[num][-1]  # stay at last known
            markers[num].set_data([x], [y])
            labels[num].set_position((x + 50, y + 50))
            frame_positions.append(lap)
        
        current_lap = max(frame_positions) if frame_positions else 0
        lap_text.set_text(f"Lap: {current_lap}")
        return list(markers.values()) + list(labels.values()) + [lap_text]
    
    ani = animation.FuncAnimation(fig, update, frames=len(timeline), interval=100, blit=True)
    ax.legend()
    plt.show()


if __name__ == "__main__":
    draw_race(CSV_FILE)