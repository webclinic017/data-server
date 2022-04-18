from datetime import date
import numpy as np

from .forex import Forex
from .market_data import MarketData
from ..utils.contract import get_front_contract
from ....common.constants import FUTURES


class Margin:
    def __init__(self):
        self.cache = {}
        self.forex = Forex()
        self.market_data = MarketData()

    def adjustment_factor(self, stem, day):
        # update of adjustment factor is done yearly
        key = f"{stem} {day.year}"
        if key in self.cache:
            return self.cache[key]
        _, ric = get_front_contract(stem=stem, day=day)
        if not self.market_data.is_trading_day(day=day, ric=ric):
            return np.NaN
        row = self.market_data.bardata(ric=ric, day=day)
        ref_date = self.get_ref_date(stem)
        _, ref_ric = get_front_contract(stem=stem, day=ref_date)
        row_ref = self.market_data.bardata(ric=ref_ric, day=ref_date)
        self.cache[key] = row["Close"][0] / row_ref["Close"][0]
        return self.cache[key]

    def get_ref_date(self, stem):
        if stem in ["HTE", "MBT"]:
            return date(2021, 8, 18)
        default_ref_date = date(2020, 1, 6)
        return default_ref_date

    def overnight_initial_future(self, stem, day):
        currency = FUTURES[stem]["Currency"]
        return (
            FUTURES[stem]["OvernightInitial"]
            * self.adjustment_factor(stem, day)
            * self.forex.to_usd(currency, day)
        )

    def overnight_maintenance_future(self, stem, day):
        currency = FUTURES[stem]["Currency"]
        return (
            FUTURES[stem]["OvernightMaintenance"]
            * self.adjustment_factor(stem, day)
            * self.forex.to_usd(currency, day)
        )


if __name__ == "__main__":
    margin = Margin()
    print(margin.overnight_initial_future("ES", date(2018, 7, 2)))
    print(margin.overnight_maintenance_future("ES", date(2018, 7, 2)))
    print(margin.overnight_initial_future("GC", date(2004, 1, 2)))
    print(margin.overnight_maintenance_future("GC", date(2004, 1, 2)))
