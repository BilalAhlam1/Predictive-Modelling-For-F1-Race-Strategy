# openf1_helper.py
import requests
import pandas as pd
import time
import random

# The following class is adapted and altered from the OpenF1 API usage example in the article:
# “OpenF1 API in Action: Building a Google Colab Notebook for F1 Race Analysis”
# https://python.plainenglish.io/openf1-api-in-action-building-a-google-colab-notebook-for-f1-race-analysis-fee86c301e5b
class OpenF1API:
    """Helper class for interacting with the OpenF1 API."""
    def __init__(self):
        self.base_url = "https://api.openf1.org/v1"
        self.last_request_time = 0
        self.min_request_interval = 0.3  # Minimum 300ms between requests

    def get_data(self, endpoint, params=None, max_retries=3):
        url = f"{self.base_url}/{endpoint}"
        
        # Rate limiting: ensure minimum time between requests
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_request_interval:
            time.sleep(self.min_request_interval - elapsed)
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, params=params, timeout=15)
                self.last_request_time = time.time()
                
                # Handle 404 silently (expected for non-existent sessions)
                if response.status_code == 404:
                    return None
                
                # Handle rate limiting with exponential backoff
                if response.status_code == 429:
                    wait_time = (2 ** attempt) + random.uniform(2, 4)
                    print(f"[OpenF1API] Rate limited on {endpoint}, waiting {wait_time:.1f}s (retry {attempt+1}/{max_retries})")
                    time.sleep(wait_time)
                    continue
                
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.HTTPError as e:
                if attempt == max_retries - 1:
                    print(f"[OpenF1API] Error fetching {endpoint} : {e}")
                    return None
            except Exception as e:
                if attempt == max_retries - 1:
                    print(f"[OpenF1API] Error fetching {endpoint} : {e}")
                    return None
                time.sleep(1)
        
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