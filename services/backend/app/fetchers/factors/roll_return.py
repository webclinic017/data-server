import pandas as pd

from ..common.cache import safe_concat, stem_to_ric
from ..ohlcv import ohlcv__raw


def factor_roll_return(future, start_date, end_date):
    stem = future["Stem"]["Reuters"]
    dfms_dict = {}
    for i in range(5):
        suffix = f"c{i+1}"
        ric = stem_to_ric(stem, suffix)
        dfm, _ = ohlcv__raw(ric, start_date, end_date)
        if dfm is None:
            continue
        dfms_dict[suffix] = dfm
    frames = []
    for i in range(4):
        if f"c{i+1}" not in dfms_dict or f"c{i+2}" not in dfms_dict:
            continue
        dfm_1 = dfms_dict[f"c{i+1}"].add_suffix("_1")
        dfm_2 = dfms_dict[f"c{i+2}"].add_suffix("_2")
        dfm = safe_concat([dfm_1, dfm_2], axis=1)
        dfm.loc[:, "RollReturn"] = dfm.CLOSE_2 / dfm.CLOSE_1 - 1
        dfm = dfm[["RollReturn"]]
        arrays = [dfm.index, [stem] * len(dfm)]
        tuples = list(zip(*arrays))
        dfm.index = pd.MultiIndex.from_tuples(tuples, names=["Date", "Stem"])
        if dfm.shape[1] > 0:
            frames.append(dfm)
    if len(frames) < 1:
        return None, "Not enough data"
    dfm = safe_concat(frames, axis=1)
    dfm = dfm.mean(axis=1).to_frame()
    dfm.columns = ["RollReturn"]
    return dfm, None
