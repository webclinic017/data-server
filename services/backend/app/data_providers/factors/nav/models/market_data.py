from datetime import timedelta

import numpy as np
from pandas_market_calendars import get_calendar

from ..utils.contract import (
    get_front_contract,
    get_last_trade_date,
    get_ric_and_stem,
    get_start_date,
)
from ..utils.dates import is_weekend
from ....common.client import Client
from ....common.constants import FUTURES


LIBOR_BEFORE_2001 = 6.65125
MAXIMUM_NUMBER_OF_DAYS_BEFORE_EXPIRY = 40


client = Client()


def get_future_ohlcv_for_day(day, contract_rank=0, ric=None, stem=None):
    ric, stem = get_ric_and_stem(
        day=day, contract_rank=contract_rank, ric=ric, stem=stem
    )
    start_date = get_start_date(stem)
    if day < start_date:
        message = f"[not-started] Future {stem} starts on {start_date.isoformat()}"
        return None, {"message": message}
    last_trade_date = get_last_trade_date(ric)
    if last_trade_date is None or day > last_trade_date:
        message = f"No OHLCV for {ric} on {day.isoformat()}"
        return None, {"message": message}
    dfm = client.get_daily_ohlcv(ric, start_date, last_trade_date)
    if dfm is not None:
        index = dfm.index == day.isoformat()
        current_day_exists = np.any(index)
        days_after_exist = np.any(dfm.index > day.isoformat())
        if current_day_exists:
            return dfm.loc[index, :], None
    message = f"No OHLCV for {ric} on {day.isoformat()}"
    return None, {"message": message}


class MarketData:
    @staticmethod
    def bardata(day, contract_rank=0, ric=None, stem=None):
        ric, stem = get_ric_and_stem(
            day=day, contract_rank=contract_rank, ric=ric, stem=stem
        )
        data, err = get_future_ohlcv_for_day(
            day=day, contract_rank=contract_rank, ric=ric, stem=stem
        )
        if err:
            raise Exception(err["message"])
        data = data.fillna(value=np.nan)
        if (
            np.isnan(data.CLOSE[0])
            and not np.isnan(data.VOLUME[0])
            and not np.all(np.isnan(data[["OPEN", "HIGH", "LOW"]].values[0]))
        ):
            data.CLOSE = np.nanmedian(data[["OPEN", "HIGH", "LOW"]].values[0])
        return data

    @staticmethod
    def is_trading_day(day, contract_rank=0, ric=None, stem=None):
        if not ric and not stem:
            calendar = get_calendar("NYSE").valid_days(day, day)
            return len(calendar) == 1
        try:
            row = MarketData.bardata(
                day=day, contract_rank=contract_rank, ric=ric, stem=stem
            )
        except Exception as e:
            message = str(e)
            if "No OHLCV for" in message:
                return False
            elif "[not-started]" in message:
                return False
            raise e
        return not np.isnan(row.CLOSE[0])

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
            if MarketData.is_trading_day(day=day) and i > 0:
                number_of_business_days += 1
            i += 1
        return day
