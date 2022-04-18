from datetime import date, datetime, timedelta

import pandas as pd
import ring
import time
from tqdm import tqdm

from .common.constants import FUTURES, LETTERS
from .common.eikon import get_timeseries
from .factors.nav.utils.contract import ric_exists


def to_short_maturity(maturity):
    """Convert a long maturity (example M24) into a short maturity (M4).

    :param maturity: Maturity to be converted
    :return: Returns a short maturity
    """
    return "{}{}".format(maturity[0], maturity[2])


def stem_to_ric_from_year(stem, year, month, is_active):
    letter = LETTERS[month - 1]
    maturity = f"{letter}{(year % 100):02d}"
    formatted_maturity = to_short_maturity(maturity)
    reuters_stem = FUTURES[stem]["Stem"]["Reuters"]
    ric = f"{reuters_stem}{formatted_maturity}"
    if not is_active:
        return "{}^{}".format(ric, maturity[1])
    return ric


@ring.lru()
def to_outright(stem, year, month, day=None, is_active=False):
    if day is None:
        return stem_to_ric_from_year(stem, year, month, is_active)
    if date.today() > date(year, month, day) + timedelta(days=10):
        return stem_to_ric_from_year(stem, year, month, is_active=False)
    ric = stem_to_ric_from_year(stem, year, month, is_active=False)
    if ric_exists(ric):
        return ric
    return stem_to_ric_from_year(stem, year, month, is_active=True)


def expiry_calendar(ticker: str, start_date: datetime, end_date: datetime):
    data_dict = {}
    error_dict = {}
    cached_data = {}
    delta = end_date - start_date
    for i in tqdm(range(delta.days + 1)):
        day = start_date + timedelta(days=i)
        if LETTERS[day.month - 1] not in FUTURES[ticker].get("NormalMonths", []):
            continue
        ric = to_outright(ticker, day.year, day.month, is_active=False)
        if ric not in cached_data:
            print(f"Downloading {ric}")
            response = get_timeseries(
                rics=ric,
                fields=["OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"],
                start_date=start_date.isoformat(),
                end_date=end_date.isoformat(),
                interval="daily",
            )
            cached_data[ric] = response["data"]
            time.sleep(1)
        data = cached_data[ric]
        if data is None or len(data) == 0:
            if ric not in error_dict:
                error_dict[ric] = day.isoformat()
            continue
        dfm = pd.DataFrame(data)
        dfm["Date"] = dfm["Date"].apply(lambda x: pd.to_datetime(x, unit="ms"))
        dfm.set_index("Date", inplace=True)
        if ric not in data_dict:
            data_dict[ric] = {
                "YearMonth": dfm.index[-1].date().strftime("%Y-%m"),
                "FTD": dfm.index[0].date().isoformat(),
                "FND": None,
                "LTD": dfm.index[-1].date().isoformat(),
                "RIC": ric,
                "WeTrd": 1,
            }
    data = list(data_dict.values())
    error_message = (
        None
        if len(error_dict) == 0
        else ", ".join([f"{k}:{v}" for k, v in error_dict.items()])
    )
    return data, error_message
