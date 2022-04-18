import pandas as pd

from ..ohlcv import ohlcv__raw


def factor_currency(future, start_date, end_date):
    ric = future["CurrencyFactor"]
    dfm, error_message = ohlcv__raw(ric, start_date, end_date)
    if error_message is not None:
        return None, error_message
    dfm = dfm[["CLOSE"]]
    if ric.startswith("USD"):
        dfm.CLOSE = 1 / dfm.CLOSE
    dfm = dfm.rename(columns={"CLOSE": "CurrencyFactor"})
    stem = future["Stem"]["Reuters"]
    arrays = [dfm.index, [stem] * len(dfm)]
    tuples = list(zip(*arrays))
    dfm.index = pd.MultiIndex.from_tuples(tuples, names=["Date", "Stem"])
    return dfm, None
