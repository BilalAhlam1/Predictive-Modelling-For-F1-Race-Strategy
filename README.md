# Formula 1 Race Analytics & Prediction Dashboard

An interactive Formula 1 analytics system built with **Python** and **Streamlit**, designed to visualize historical race data and predict pit stop strategies using **machine learning**.  
The project integrates open-source F1 telemetry APIs to provide data-driven race insights, performance metrics, and strategic simulations.

---

## Features

- **Live Race Data Integration**  
  Retrieves telemetry data from the [FastF1](https://theoehrly.github.io/Fast-F1/) and/or [OpenF1](https://openf1.org) APIs including lap times, tyre compounds, pit stops, and weather conditions.

- **Historical Replay Dashboard**  
  Visualizes lap-by-lap driver positions, tyre degradation, and race progress with interactive charts powered by **Plotly** and **Streamlit**.

- **Predictive Modelling**  
  Uses machine learning (e.g., regression models) to predict pit stop timing and strategy outcomes.  
  Displays **confidence intervals** for predictions to indicate uncertainty.

- **Simulation Mode**  
  Allows users to test alternative race strategies and compare predicted outcomes against historical results.

- **Offline Data Support**  
  Local caching ensures reliability even when API access is unavailable.

---

## Project Objectives

1. Collect and clean historical F1 race data from open APIs.  
2. Build an interactive web-based dashboard for race visualization.  
3. Train predictive models to estimate pit stop timing and race outcomes.  
4. Evaluate model accuracy using metrics such as RMSE, MAE, and R².  
5. Present analytical insights through intuitive charts and reports.

---

## Tech Stack

- **Frontend/UI**: [Streamlit](https://streamlit.io)  
- **Backend**: Python 3.11  
- **Data Handling**: Pandas, NumPy  
- **Visualization**: Plotly, Matplotlib  
- **Machine Learning**: Scikit-learn  
- **Data Source**: OpenF1 / FastF1 APIs  

---

## Example Outputs

- Lap Time vs. Tyre Compound Graphs  
- Predicted Pit Stop Laps with Confidence Intervals  
- Comparison of Real vs. Predicted Strategies  
- Race Replay with Driver Position Animations  

---

## Legal, Ethical & Licensing Notes

- This project is **unofficial** and **not associated with Formula 1 or its affiliates**.  
  > F1, FORMULA ONE, FORMULA 1, FIA FORMULA ONE WORLD CHAMPIONSHIP, GRAND PRIX  
  and related marks are trademarks of **Formula One Licensing B.V.**

- Data from OpenF1/FastF1 is used under open-source, non-commercial academic licenses.  
- No personal data is collected; all analyses are for **educational and research purposes**.

---

## Installation & Usage

### 1. Clone the Repository
```bash
git clone https://github.com/BilalAhlam1/Predictive-Modelling-For-F1-Race-Strategy.git
cd Predictive-Modelling-For-F1-Race-Strategy
