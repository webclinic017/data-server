import numpy as np
import pandas as pd

from ..common.cache import cache_in_s3, json_data_to_df, safe_concat
from ..common.eikon import get_data
from ..ohlcv import ohlcv__raw


@cache_in_s3("daily-dividend", lambda x: json_data_to_df(x, version="v2"))
def dividend__raw(ric, start_date, end_date):
    return get_data(
        instruments=ric,
        fields=["TR.Index_DIV_YLD_RTRS.Date", "TR.Index_DIV_YLD_RTRS"],
        parameters={"SDate": start_date.isoformat(), "EDate": end_date.isoformat()},
    )


def dividend(future, start_date, end_date):
    stem = future["Stem"]["Reuters"]
    ric = future["CarryFactor"]["ExpectedDividend"]
    dfm, error_message = dividend__raw(ric, start_date, end_date)
    if error_message is not None:
        return None, error_message
    not_null_dates = dfm.index.map(lambda x: not pd.isnull(x))
    dfm = dfm.loc[not_null_dates, :]
    dfm.index = pd.to_datetime(dfm.index, format="%Y-%m-%d")
    arrays = [dfm.index, [stem] * len(dfm)]
    tuples = list(zip(*arrays))
    dfm.index = pd.MultiIndex.from_tuples(tuples, names=["Date", "Stem"])
    column_name = "Calculated Index Dividend Yield"
    dfm = dfm[[column_name]].rename(columns={column_name: "DividendYield"})
    dfm.index.map(lambda x: np.isnan([0]))
    return dfm, None


def risk_free_rate(future, start_date, end_date):
    stem = future["Stem"]["Reuters"]
    dfm, error_message = ohlcv__raw("US3MT=RR", start_date, end_date)
    if error_message is not None:
        return None, error_message
    dfm = dfm[["CLOSE"]].rename(columns={"CLOSE": "RiskFreeRate"})
    arrays = [dfm.index, [stem] * len(dfm)]
    tuples = list(zip(*arrays))
    dfm.index = pd.MultiIndex.from_tuples(tuples, names=["Date", "Stem"])
    return dfm, None


def factor_carry_equity(future, start_date, end_date):
    dfm_dividend, error_message = dividend(future, start_date, end_date)
    if error_message is not None:
        return None, error_message
    dfm_risk_free_rate, error_message = risk_free_rate(future, start_date, end_date)
    if error_message is not None:
        return None, error_message
    dfm = safe_concat([dfm_dividend, dfm_risk_free_rate], axis=1)
    dfm["CarryFactor"] = (dfm.DividendYield - dfm.RiskFreeRate) / 100
    return dfm[["CarryFactor"]], None
