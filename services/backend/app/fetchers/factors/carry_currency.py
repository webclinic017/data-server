import pandas as pd

from ..ohlcv import ohlcv__raw


def factor_carry_currency(future, start_date, end_date):
    ric = future["CarryFactor"]["LocalInterestRate"]
    dfm, error_message = ohlcv__raw(ric, start_date, end_date)
    if error_message is not None:
        return None, error_message
    stem = future["Stem"]["Reuters"]
    arrays = [dfm.index, [stem] * len(dfm)]
    tuples = list(zip(*arrays))
    dfm.index = pd.MultiIndex.from_tuples(tuples, names=["Date", "Stem"])
    dfm = dfm[["CLOSE"]].rename(columns={"CLOSE": "CarryFactor"}) / 100
    return dfm, None
