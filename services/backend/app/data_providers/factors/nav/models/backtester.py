"""
Backtester
"""

from datetime import timedelta
import os
from pprint import pprint
import uuid

import numpy as np
import pandas as pd
from tqdm import tqdm

from .broker import Broker
from .market_data import MarketData
from ..utils.contract import get_chain
from ..utils.dates import is_weekend
from ....common.constants import FUTURE_TYPE


TWELVE_MONTHS = 250


class Backtester:
    def __init__(
        self,
        stems,
        start_date,
        end_date,
        cash,
        leverage,
        live,
        instrument_type=FUTURE_TYPE,
        no_check=False,
        plot=True,
        suffix="",
    ):
        self.broker = Broker(cash, live, no_check)
        self.market_data = MarketData()
        self.cash = cash
        self.data = []
        self.dates = []
        self.day = None
        self.end_date = end_date
        self.instrument_type = instrument_type
        self.leverage = leverage
        self.live = live
        self.nav = cash
        self.plot = plot
        self.start_date = start_date
        self.stems = stems
        self.suffix = suffix

    def compute_kelly(self):
        dfm = pd.DataFrame(data=self.data, index=self.dates)
        returns = np.diff(np.log(dfm.Nav))
        kelly = np.nanmean(returns) / np.power(np.nanstd(returns), 2)
        return kelly

    def compute_mean(self):
        dfm = pd.DataFrame(data=self.data, index=self.dates)
        returns = np.diff(np.log(dfm.Nav))
        mean = np.nanmean(returns) * TWELVE_MONTHS
        return mean

    def plot_nav(self):
        dfm = pd.DataFrame(data=self.data, index=self.dates)
        fname = ",".join(self.stems)
        if len(fname) > 30:
            fname = str(uuid.uuid3(uuid.NAMESPACE_URL, fname))
        filename = (
            fname + f".{self.leverage}.{self.end_date.isoformat()}.{self.suffix}.png"
        )
        path = os.path.join(os.getenv("HOME"), "Downloads", filename)
        dfm[["Nav"]].plot(logy=True).get_figure().savefig(path)

    def compute_sharpe_ratio(self):
        dfm = pd.DataFrame(data=self.data, index=self.dates)
        returns = np.diff(np.log(dfm.Nav))
        sharpe_ratio = np.nanmean(returns) / np.nanstd(returns) * np.sqrt(250)
        return sharpe_ratio

    def compute_std(self):
        dfm = pd.DataFrame(data=self.data, index=self.dates)
        returns = np.diff(np.log(dfm.Nav))
        std = np.nanstd(returns) * np.sqrt(TWELVE_MONTHS)
        return std

    def has_not_enough_active_contracts(self):
        for stem in self.stems:
            active_contracts = get_chain(stem, self.day)
            if active_contracts.shape[0] < 2:
                return stem
        return None

    def run(self):
        delta = self.end_date - self.start_date
        for i in tqdm(range(delta.days + 1)):
            self.day = self.start_date + timedelta(days=i)
            if self.instrument_type == FUTURE_TYPE:
                not_enough_active_contracts = self.has_not_enough_active_contracts()
                if not_enough_active_contracts is not None:
                    raise Exception(
                        f"Update future-expiry/{not_enough_active_contracts}.csv in Minio"
                    )
            if is_weekend(self.day):
                continue
            self.broker.next(self.day)
            self.next()
            self.next_indicators()
            self.dates.append(self.day)
            nav = self.broker.nav
            if not np.isnan(nav):
                self.nav = nav
            data = {}
            data["Nav"] = nav
            self.data.append(data)
        if not self.live and self.plot:
            self.plot_nav()
        kelly = self.compute_kelly()
        mean = self.compute_mean()
        sharpe = self.compute_sharpe_ratio()
        std = self.compute_std()
        pprint(
            {
                "mean": mean,
                "std": std,
                "kelly": kelly,
                "sharpe": sharpe,
            }
        )

    def next(self):
        pass

    def next_indicators(self):
        pass
