from .ohlcv import ohlcv__raw
import pandas as pd


def factor_currency(future, start_date, end_date):
    ric = future['CurrencyFactor']
    dfm = ohlcv__raw(ric, start_date, end_date)
    dfm = dfm[['CLOSE']]
    if ric.startswith('USD'):
        dfm.CLOSE = 1 / dfm.CLOSE
    dfm = dfm.rename(columns={'CLOSE': 'CurrencyFactor'})
    stem = future['Stem']['Reuters']
    arrays = [dfm.index, [stem] * len(dfm)]
    tuples = list(zip(*arrays))
    dfm.index = pd.MultiIndex.from_tuples(tuples, names=['Date', 'Stem'])
    return dfm
