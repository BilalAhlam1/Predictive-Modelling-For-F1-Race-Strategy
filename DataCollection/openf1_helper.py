# openf1_helper.py
import requests
import pandas as pd

class OpenF1API:
    """Helper class for interacting with the OpenF1 API."""
    def __init__(self):
        self.base_url = "https://api.openf1.org/v1"

    def get_data(self, endpoint, params=None):
        url = f"{self.base_url}/{endpoint}"
        try:
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"[OpenF1API] Error fetching {endpoint} : {e}")
            return None

    def get_dataframe(self, endpoint, params=None):
        data = self.get_data(endpoint, params)
        if data:
            # If the API returns a dict with nested payload, try to extract common keys:
            if isinstance(data, dict):
                # If the top-level is {"data": [...] } or similar, try to find the list
                for v in data.values():
                    if isinstance(v, list):
                        try:
                            return pd.DataFrame(v)
                        except Exception:
                            break
                # fallback: attempt to wrap dict into DataFrame
                try:
                    return pd.DataFrame([data])
                except Exception:
                    return pd.DataFrame()
            else:
                try:
                    return pd.DataFrame(data)
                except Exception:
                    return pd.DataFrame()
        return pd.DataFrame()

# module-level API client
api = OpenF1API()