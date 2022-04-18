from datetime import timedelta

import numpy as np
from pandas_market_calendars import get_calendar
import ring

from ..utils.contract import (
    get_front_contract,
    get_first_trade_date,
    get_last_trade_date,
)
from ..utils.dates import is_weekend
from ....common.client import Client
from ....common.constants import FUTURES


LIBOR_BEFORE_2001 = 6.65125
MAXIMUM_NUMBER_OF_DAYS_BEFORE_EXPIRY = 40


client = Client()


@ring.lru()
def get_future_ohlcv(ric, start_date, end_date):
    dfm = client.get_daily_ohlcv(ric, start_date, end_date)
    dfm.reset_index(drop=False, inplace=True)
    dfm.Date = dfm.Date.apply(lambda x: x[:10])
    dfm.drop(columns=["RIC"], inplace=True)
    dfm.set_index("Date", drop=True, inplace=True)
    return dfm


def get_future_ohlcv_for_day(day, ric=None):
    first_trade_date = get_first_trade_date(ric)
    last_trade_date = get_last_trade_date(ric)
    if (
        first_trade_date is None
        or day < first_trade_date
        or last_trade_date is None
        or day > last_trade_date
    ):
        message = f"No OHLCV for {ric} on {day.isoformat()}"
        return None, {"message": message}
    dfm = get_future_ohlcv(ric, first_trade_date, last_trade_date)
    if dfm is not None:
        index = dfm.index == day.isoformat()
        current_day_exists = np.any(index)
        if current_day_exists:
            return dfm.loc[index, :], None
    message = f"No OHLCV for {ric} on {day.isoformat()}"
    return None, {"message": message}


class MarketData:
    @staticmethod
    def bardata(day, ric=None):
        data, err = get_future_ohlcv_for_day(day=day, ric=ric)
        if err:
            raise Exception(err["message"])
        data = data.fillna(value=np.nan)
        if (
            np.isnan(data.Close[0])
            and not np.isnan(data.VOLUME[0])
            and not np.all(np.isnan(data[["Open", "High", "Low"]].values[0]))
        ):
            data.Close = np.nanmedian(data[["Open", "High", "Low"]].values[0])
        return data

    @staticmethod
    def is_trading_day(day, ric=None):
        if ric is None:
            return False
        try:
            row = MarketData.bardata(day=day, ric=ric)
        except Exception as e:
            message = str(e)
            if "No OHLCV for" in message:
                return False
            elif "[not-started]" in message:
                return False
            raise e
        return not np.isnan(row.Close[0])

    def should_roll_today(self, day, stem):
        front_ltd, front_ric = get_front_contract(day=day, stem=stem)
        if day + timedelta(days=MAXIMUM_NUMBER_OF_DAYS_BEFORE_EXPIRY) < front_ltd:
            return False
        future = FUTURES.get(stem, {})
        roll_offset_from_reference = timedelta(
            days=future.get("RollOffsetFromReference", -31)
        )
        delta = front_ltd - day + roll_offset_from_reference
        _, next_ric = get_front_contract(day=day, stem=stem)
        for i in range(1, delta.days + 1):
            _day = day + timedelta(days=i)
            is_better_day_to_roll = (
                not is_weekend(_day)
                and self.is_trading_day(day=_day, ric=front_ric)
                and self.is_trading_day(day=_day, ric=next_ric)
            )
            if is_better_day_to_roll:
                return False
        return True

    @staticmethod
    def get_start_day(first_trading_day, window):
        i = 0
        number_of_business_days = 0
        while number_of_business_days < window:
            day = first_trading_day + timedelta(days=-i)
            is_trading_day = len(get_calendar("NYSE").valid_days(day, day)) == 1
            if is_trading_day and i > 0:
                number_of_business_days += 1
            i += 1
        return day
