import json
import os
import tempfile

import pandas as pd
import quandl as qdl

from ..common.cache import download_from_s3, json_data_to_df
from ..common.constants import FUTURES
from ..common.minio import exists_object, fget_object, put_object


# What are the different COT types
COT_TYPES = {
    "F": "FuturesOnly",
    "FO": "FuturesAndOptions",
    "F_L": "FuturesOnlyLegacy",
    "FO_L": "FuturesAndOptionsLegacy",
}

# Quandl API Key
qdl.ApiConfig.api_key = os.getenv("QUANDL_API_KEY")


def download_commitment_of_traders(stem, cot_type="F"):
    if cot_type in COT_TYPES:
        qdl_code = FUTURES[stem]["COT"]
        df = qdl.get("CFTC/{}_{}_ALL".format(qdl_code, cot_type))
    else:
        raise Exception("COT Type {} not defined!".format(cot_type))
    return df


def get_commitment_of_traders(stem, cot_type="F"):
    """
    Get the cot data and cache it (Refresh it if file is older than 7 days).
    COT Types can be:
        -- F: Futures Only
        -- FO: Futures And Options
        -- F_L: Futures Only Legacy
        -- FO_L Futures And Options Only

    :param stem: str -  Market stem (customized)
    :param cot_type: String COT Type
    :return: Dataframe with COT data
    """
    bucket_name = "daily-cot"
    object_name = f"{stem}.json"
    df_quandl = download_commitment_of_traders(stem=stem, cot_type=cot_type)
    if exists_object(bucket_name, object_name):
        data, _ = download_from_s3(bucket_name, object_name)
        df_s3 = json_data_to_df(data, version="v1")
        df_quandl = df_quandl.loc[
            df_quandl.index > df_s3.index[-1],
        ]
        df_concat = pd.concat([df_s3, df_quandl], sort=True)
    else:
        df_concat = df_quandl
    data = json.loads(df_concat.reset_index(level=0).to_json(orient="records"))
    response = {"data": data, "error": {}}
    temp_dir = tempfile.TemporaryDirectory()
    path = os.path.join(temp_dir.name, object_name)
    with open(path, "w") as f:
        json.dump(response, f)
    put_object(path, bucket_name)
    temp_dir.cleanup()
    return json_data_to_df(data, version="v1")


def factor_cot(future, start_date, end_date):
    stem = future["Stem"]["Reuters"]
    dfm = get_commitment_of_traders(stem, cot_type="F")
    columns = [
        "Money Manager Longs",
        "Money Manager Shorts",
        "Money Manager Spreads",
        "Non Reportable Longs",
        "Non Reportable Shorts",
        "Open Interest",
        "Other Reportable Longs",
        "Other Reportable Shorts",
        "Other Reportable Spreads",
        "Producer/Merchant/Processor/User Longs",
        "Producer/Merchant/Processor/User Shorts",
        "Swap Dealer Longs",
        "Swap Dealer Shorts",
        "Swap Dealer Spreads",
        "Total Reportable Longs",
        "Total Reportable Shorts",
    ]
    dfm = dfm[columns]
    arrays = [dfm.index, [stem] * len(dfm)]
    tuples = list(zip(*arrays))
    dfm.index = pd.MultiIndex.from_tuples(tuples, names=["Date", "Stem"])
    return dfm, None
