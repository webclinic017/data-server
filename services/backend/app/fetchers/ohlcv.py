import pandas as pd

from .common.cache import cache_in_s3, json_data_to_df
from .common.eikon import get_timeseries


@cache_in_s3("daily-ohlcv", json_data_to_df)
def ohlcv__raw(ric, start_date, end_date):
    return get_timeseries(
        ric, start_date=start_date.isoformat(), end_date=end_date.isoformat()
    )


def ohlcv(ric, start_date, end_date):
    dfm, error_message = ohlcv__raw(ric, start_date, end_date)
    if error_message is not None:
        return None, error_message
    dfm = dfm[["OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]]
    dfm = dfm.rename(
        columns={
            "OPEN": "Open",
            "HIGH": "High",
            "LOW": "Low",
            "CLOSE": "Close",
            "VOLUME": "Volume",
        }
    )
    arrays = [dfm.index, [ric] * len(dfm)]
    tuples = list(zip(*arrays))
    dfm.index = pd.MultiIndex.from_tuples(tuples, names=["Date", "RIC"])
    return dfm, None
