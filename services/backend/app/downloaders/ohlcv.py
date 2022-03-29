from common.data.database import json_data_to_df
from common.data.eikon import get_timeseries
from .utils import save_in_s3, stem_to_ric
import pandas as pd


@save_in_s3('daily-ohlcv', json_data_to_df)
def ohlcv__raw(ric, start_date, end_date):
    return get_timeseries(
        ric, start_date=start_date.isoformat(), end_date=end_date.isoformat())


def ohlcv(future, start_date, end_date):
    stem = future['Stem']['Reuters']
    ric = stem_to_ric(stem, 'c1')
    dfm, error_message = ohlcv__raw(ric, start_date, end_date)
    if dfm is None:
        return None, error_message
    dfm = dfm[['OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME']]
    dfm = dfm.rename(columns={
        'OPEN': 'Open',
        'HIGH': 'High',
        'LOW': 'Low',
        'CLOSE': 'Close',
        'VOLUME': 'Volume'})
    arrays = [dfm.index, [stem] * len(dfm)]
    tuples = list(zip(*arrays))
    dfm.index = pd.MultiIndex.from_tuples(tuples, names=['Date', 'Stem'])
    return dfm, None
