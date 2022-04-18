import pandas as pd

from ..common.cache import safe_concat, stem_to_ric
from ..ohlcv import ohlcv__raw


def factor_carry_commodity(future, start_date, end_date):
    stem = future["Stem"]["Reuters"]
    dfm_1, error_message = ohlcv__raw(stem_to_ric(stem, "c1"), start_date, end_date)
    if error_message is not None:
        return None, error_message
    dfm_1 = dfm_1.add_suffix("_c1")
    dfm_2, error_message = ohlcv__raw(stem_to_ric(stem, "c2"), start_date, end_date)
    if error_message is not None:
        return None, error_message
    dfm_2 = dfm_2.add_suffix("_c2")
    dfm = safe_concat([dfm_1, dfm_2], axis=1)
    arrays = [dfm.index, [stem] * len(dfm)]
    tuples = list(zip(*arrays))
    dfm.index = pd.MultiIndex.from_tuples(tuples, names=["Date", "Stem"])
    dfm["CarryFactor"] = dfm.CLOSE_c2 / dfm.CLOSE_c1 - 1
    return dfm[["CarryFactor"]], None
