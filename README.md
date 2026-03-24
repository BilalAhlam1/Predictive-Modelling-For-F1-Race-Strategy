Formula 1 Race Analytics & Prediction Dashboard
BSc Computer Science Final Year Project

An interactive Formula 1 analytics system built using Python and Streamlit. The system visualises historical race data and predicts and evaluates pit stop strategies using machine learning. It integrates open-source F1 telemetry APIs to provide data-driven race insights, performance metrics, and strategic simulations. The system is designed to support race strategy analysis by combining machine learning-based pace prediction with a physics-driven simulation of race dynamics.

-----

Quick Start:

1. Open the project in VS Code  
2. In the terminal, run:
   cd RaceVisualiser
   streamlit run app.py
3. Use the dashboard (http://localhost:8501) to explore race simulations and predictions  

Note: No additional configuration is required if the included database is present (found at DatabaseConnection/f1_strategy.db). Dependency installation can be found in the Installation and Usage Guide.

-----

Example User Journey (Usability Guidance)

To support usability and reduce the learning curve for first-time users, a typical interaction flow is outlined below:
1.    Launch Application
        Upon running the system, the home page is displayed. This serves as the main entry point for all analysis features.
2.    Select a Race Session
        From the home page, select a race (e.g., Lusail) from the available dataset. This initial selection determines the dataset used across all subsequent views.
3.    Explore Race Replay and Analytics
        Navigate to the “Race Replay” page using the left-hand sidebar.
        This view provides:
        - Lap-by-lap position tracking
        - Tyre usage and degradation trends
        - Interactive visualisations of race progression
        Note: Initial loading may take time as the race simulation is pre-rendered for accuracy.
4.    Evaluate Alternative Strategies
        Navigate to the “Strategy Prediction” page via the sidebar. (See Known Limitations section for sprint race support details)
        This module allows users to:
        - Modify pit stop timing
        - Adjust strategy parameters
        - Compare simulated outcomes against historical results
        Default values reflect the original historical race strategy, enabling direct comparison with alternative scenarios.
-----

The system supports both exploratory analysis and what-if strategy evaluation through an integrated visual and simulation pipeline.

Features
- Live Race Data Integration
    Retrieves telemetry data from the OpenF1 API, including lap times, tyre compounds, pit stops, and weather conditions.
- Historical Replay Dashboard
    Visualises lap-by-lap driver positions, tyre degradation, and race progression using interactive charts built with Plotly and Streamlit.
- Predictive Modelling
    Uses a Random Forest Regression model to predict pit stop timing and strategy outcomes, capturing interactions such as fuel burn and tyre degradation.
- Simulation Mode
    Allows users to test alternative race strategies using a custom physics-based traffic interaction layer. Predicted outcomes can be compared against historical race results.
- Offline Data Support
    A local SQLite database cache improves reliability and performance, allowing fast loading and continued use when API access is unavailable.

-----

Installation and Usage Guide
1.    Extract Project Files
        Extract the provided .zip file and open the folder in Visual Studio Code (File → Open Folder).

2.    Install Dependencies
        Open a terminal and run:

        python -m venv venv

        Windows:
        venv\Scripts\activate

        macOS/Linux:
        source venv/bin/activate

        pip install -r requirements.txt

3.    Database Setup
        Check if the file “f1_strategy.db” exists in the DatabaseConnection folder. (This is included in this .zip)
        If it exists, skip this step.

        If not, run:

        cd DatabaseConnection
        python createDatabase.py
        cd ..

4.    Launch Application
        cd RaceVisualiser
        streamlit run app.py

        This will launch on http://localhost:8501

Note:
A cached database is included. If the database is empty, initial data ingestion may take up to 20 minutes as the system retrieves and processes over 800 laps of telemetry data. Subsequent runs are significantly faster due to caching, though new race sessions may still require initial processing.

-----

Advanced Configuration (Optional)

For reproducibility, the dataset is fixed to a predefined snapshot of race data.

If you wish to retrieve the most recent race data instead, the following lines can be modified:
- In DataCollection/storeRaceData.py, comment out line 347
- In DataCollection/storeMLData.py, comment out line 273

These lines are intentionally kept active to ensure consistent and reproducible results during evaluation.

!! Modifying these settings may result in differences between reproduced results and those presented in the report.

-----

Submitted Folder Structure

This submission is organised into five main components:

- DatabaseConnection: Database setup and management scripts, plus the included SQLite cache used by the app ([createDatabase.py](DatabaseConnection/createDatabase.py), [databaseManager.py](DatabaseConnection/databaseManager.py), [f1_strategy.db](DatabaseConnection/f1_strategy.db)).
- DataCollection: OpenF1 data ingestion and preprocessing scripts for race, weather, and ML datasets ([openf1_helper.py](DataCollection/openf1_helper.py), [storeRaceData.py](DataCollection/storeRaceData.py), [storeMLData.py](DataCollection/storeMLData.py), [weatherData.py](DataCollection/weatherData.py)).
- RaceVisualiser: Main Streamlit application and UI pages for home, race replay, and strategy prediction ([app.py](RaceVisualiser/app.py), [Pages/dashboardHome.py](RaceVisualiser/Pages/dashboardHome.py), [Pages/raceReplay.py](RaceVisualiser/Pages/raceReplay.py), [Pages/strategyPrediction.py](RaceVisualiser/Pages/strategyPrediction.py)).
- TrainingModel: Model training notebooks, validation outputs, and saved model artifacts used for prediction ([train.ipynb](TrainingModel/train.ipynb), [simulation_logic.ipynb](TrainingModel/simulation_logic.ipynb), [models/](TrainingModel/models)).
- Testing: Utility scripts used to validate and reset database/testing workflows ([clearDatabaseTable.py](Testing/clearDatabaseTable.py), [databaseQueryValidation.py](Testing/databaseQueryValidation.py)).

Supporting root files include [README.md](README.md) (documentation) and [requirements.txt](requirements.txt) (dependencies).

-----

Project Objectives
1.    Collect, clean, and standardise historical Formula 1 race data from open APIs.
2.    Develop an interactive dashboard for race visualisation.
3.    Train machine learning models to estimate clean-air lap pace and race outcomes.
4.    Evaluate model performance using RMSE, MAE, and R².
5.    Present insights through visualisations, coordinate-based track mapping, and comparative analysis.

-----

Expected Output:
- Interactive dashboard opens in browser  
- Race visualisation with lap-by-lap progression  
- Strategy simulation controls available  
- Charts showing lap times, tyre usage, and predictions  
- Ability to compare simulated strategies against historical race outcomes

-----

Known Limitations:
- Sprint race sessions are supported for race replay and visualisation, but are not fully compatible with the strategy prediction module due to differences in race format and pit stop behaviour
- Initial data ingestion may be slow due to API data retrieval
- Simulation accuracy depends on available telemetry granularity (e.g., no ERS or driver input data)

-----

Tech Stack
- Frontend / UI: Streamlit
- Backend: Python 3.11
- Data Handling: Pandas, NumPy, SQLAlchemy
- Visualisation: Plotly, Matplotlib
- Machine Learning: Scikit-learn
- Data Source: OpenF1 API

-----

Legal and Ethical Notes
- This project is unofficial and not associated with Formula 1 or its affiliates.
- F1, Formula One, Formula 1, FIA Formula One World Championship, and Grand Prix are trademarks of Formula One Licensing B.V.
- All data is sourced from OpenF1 under open-source, non-commercial academic use.
- No personal data is collected. The system is developed strictly for educational and research purposes.