
from .ohlcv import ohlcv__raw
import pandas as pd


def factor_carry_currency(future, start_date, end_date):
    ric = future['CarryFactor']['LocalInterestRate']
    dfm = ohlcv__raw(ric, start_date, end_date)
    stem = future['Stem']['Reuters']
    arrays = [dfm.index, [stem] * len(dfm)]
    tuples = list(zip(*arrays))
    dfm.index = pd.MultiIndex.from_tuples(tuples, names=['Date', 'Stem'])
    dfm = dfm[['CLOSE']].rename(columns={'CLOSE': 'CarryFactor'}) / 100
    return dfm, None
