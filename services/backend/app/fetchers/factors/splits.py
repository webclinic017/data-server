import numpy as np
import pandas as pd
from tqdm import tqdm

from ..ohlcv import ohlcv__raw


def business_conditions(future, start_date, end_date):
    column = "BusinessConditions"
    stem = future["Stem"]["Reuters"]
    window_long = 200
    window_short = 50
    dfm, error_message = ohlcv__raw(".SPX", start_date, end_date)
    if error_message is not None:
        return None, error_message
    arrays = [dfm.index, [stem] * len(dfm)]
    tuples = list(zip(*arrays))
    dfm.index = pd.MultiIndex.from_tuples(tuples, names=["Date", "Stem"])
    shift_period = -int(window_short / 2)
    dfm[column] = np.sign(
        dfm.CLOSE.shift(shift_period).rolling(window=window_short).mean()
        - dfm.CLOSE.shift(shift_period).rolling(window=window_long).mean()
    ).fillna(method="ffill")
    return dfm[[column]]


def factor_splits(future, start_date, end_date):
    """ """
    column = "TrainingSets"
    minimum_number_of_months_between_sets = 3
    maximum_number_of_months_in_set = 10
    sequence = ["train", "dev", "train", "test", "train"]
    sequence_index = 0
    previous_row = None
    number_of_months_since_previous_change = 0
    dfm = business_conditions(future, start_date, end_date)
    dfm.loc[:, column] = ""
    for index, row in tqdm(dfm.iterrows(), total=len(dfm)):
        if pd.isna(row.BusinessConditions):
            continue
        if (
            (
                previous_row is not None
                and row.BusinessConditions != previous_row.BusinessConditions
            )
            or number_of_months_since_previous_change
            > maximum_number_of_months_in_set + minimum_number_of_months_between_sets
        ):
            sequence_index = (sequence_index + 1) % len(sequence)
            number_of_months_since_previous_change = 0
        previous_row = row
        number_of_months_since_previous_change += 1
        if (
            number_of_months_since_previous_change
            > minimum_number_of_months_between_sets
        ):
            dfm.loc[index, column] = sequence[sequence_index]
    return dfm, None
