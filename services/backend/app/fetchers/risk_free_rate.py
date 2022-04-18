import pandas as pd

from .common.cache import cache_in_s3, json_data_to_df
from .common.eikon import get_data


@cache_in_s3("daily-risk-free-rate", lambda x: json_data_to_df(x, version="v2"))
def risk_free_rate__raw(ric, start_date, end_date):
    return get_data(
        instruments=ric,
        fields=["TR.FIXINGVALUE.Date", "TR.FIXINGVALUE"],
        parameters={"SDate": start_date.isoformat(), "EDate": end_date.isoformat()},
    )


def risk_free_rate(ric, start_date, end_date):
    dfm, error_message = risk_free_rate__raw(ric, start_date, end_date)
    if error_message is not None:
        return None, error_message
    dfm = dfm[["Fixing Value"]]
    dfm = dfm.rename(columns={"Fixing Value": "FixingValue"})
    arrays = [dfm.index, [ric] * len(dfm)]
    tuples = list(zip(*arrays))
    dfm.index = pd.MultiIndex.from_tuples(tuples, names=["Date", "RIC"])
    return dfm, None
