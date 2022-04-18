import os

import pandas as pd
import requests


class Client:
    def __init__(self):
        self.headers = {"Authorization": os.getenv("DATA_SECRET_KEY")}

    def get_daily_factor(self, path, ticker, start_date, end_date):
        response = requests.get(
            f"http://localhost:8000/daily/factor/{path}",
            headers=self.headers,
            params={
                "ticker": ticker,
                "start_date": start_date,
                "end_date": end_date,
            },
        )
        response_json = response.json()
        error = response_json["error"]
        if error is not None:
            print(error)
        data = response_json["data"]
        if data is None:
            return
        dfm = pd.DataFrame.from_dict(data)
        dfm = dfm.set_index(["Date", "Stem"])
        return dfm

    def get_daily_ohlcv(self, ric, start_date, end_date):
        response = requests.get(
            "http://localhost:8000/daily/ohlcv",
            headers=self.headers,
            params={
                "ric": ric,
                "start_date": start_date,
                "end_date": end_date,
            },
        )
        response_json = response.json()
        error = response_json["error"]
        if error is not None:
            print(error)
        data = response_json["data"]
        if data is None:
            return
        dfm = pd.DataFrame.from_dict(data)
        dfm = dfm.set_index(["Date", "RIC"])
        return dfm

    def get_daily_risk_free_rate(self, ric, start_date, end_date):
        response = requests.get(
            "http://localhost:8000/daily/risk-free-rate",
            headers=self.headers,
            params={
                "ric": ric,
                "start_date": start_date,
                "end_date": end_date,
            },
        )
        response_json = response.json()
        error = response_json["error"]
        if error is not None:
            print(error)
        data = response_json["data"]
        if data is None:
            return
        dfm = pd.DataFrame.from_dict(data)
        dfm = dfm.set_index(["Date", "RIC"])
        return dfm

    def get_health_ric(self, ric):
        response = requests.get(
            "http://localhost:8000/health/ric",
            headers=self.headers,
            params={
                "ric": ric,
            },
        )
        data = response.json().get("data", False)
        return data

    def get_tickers(self):
        response = requests.get("http://localhost:8000/tickers", headers=self.headers)
        data = response.json().get("data", [])
        return data
