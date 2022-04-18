import numpy as np
import pandas as pd

from ..common.cache import safe_concat
from ..ohlcv import ohlcv__raw


def factor_carry_bond(future, start_date, end_date):
    stem = future["Stem"]["Reuters"]
    dfm_5, error_message = ohlcv__raw(
        future["CarryFactor"]["GovernmentInterestRate5Y"], start_date, end_date
    )
    if error_message is not None:
        return None, error_message
    dfm_5 = dfm_5.add_suffix("_5")
    dfm_10, error_message = ohlcv__raw(
        future["CarryFactor"]["GovernmentInterestRate10Y"], start_date, end_date
    )
    if error_message is not None:
        return None, error_message
    dfm_10 = dfm_10.add_suffix("_10")
    dfm_rfr, error_message = ohlcv__raw("US3MT=RR", start_date, end_date)
    if error_message is not None:
        return None, error_message
    dfm_rfr = dfm_rfr.add_suffix("_rfr")
    dfm = safe_concat([dfm_5, dfm_10, dfm_rfr], axis=1)
    arrays = [dfm.index, [stem] * len(dfm)]
    tuples = list(zip(*arrays))
    dfm.index = pd.MultiIndex.from_tuples(tuples, names=["Date", "Stem"])
    dfm["CarryFactor"] = (
        np.power(1 + dfm.CLOSE_10 / 100, 10)
        / (np.power(1 + dfm.CLOSE_rfr / 100, 5) * np.power(1 + dfm.CLOSE_5 / 100, 5))
        - 1
    )
    return dfm[["CarryFactor"]], None
