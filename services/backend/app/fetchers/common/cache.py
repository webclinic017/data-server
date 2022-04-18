"""
Caching management module in Minio.
"""

from datetime import datetime, timedelta
from http.client import TOO_MANY_REQUESTS
import json
import os
import tempfile

from dateutil.relativedelta import relativedelta
import pandas as pd
import ring
from tqdm import tqdm

from .minio import (
    exists_object,
    fget_object,
    make_bucket_if_not_exists,
    put_object,
    stat_object,
)


EIKON_NOT_RUNNING = "Eikon not running"
TOO_MANY_REQUESTS = "Too many requests"
FUNCTIONS_RETURNING_STRING_INDEX = ["dividend__raw", "risk_free_rate__raw"]


@ring.lru()
def download_from_s3(bucket_name: str, object_name: str):
    """
    Downloads cached object in Minio if it exists.

    Parameters
    ----------
        bucket_name: string

        object_name: string

        version: string

    Returns
    -------
        dict
            The data of the object and the error.
    """
    if not exists_object(bucket_name, object_name):
        return None, None
    with tempfile.NamedTemporaryFile(suffix=".json") as temp_file:
        fget_object(bucket_name, object_name, temp_file.name)
        with open(temp_file.name, "r") as handler:
            response = json.load(handler)
        return response["data"], response["error"]


def ensure_dir(file_path: str):
    """
    Creates a directory if it doesn't exist yet.

    Parameters
    ----------
        file_path: string

    Returns:
        string
            The path given as argument.
    """
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)
    return file_path


def json_data_to_df(data: dict, version="v1"):
    """
    Converts a dict to a data frame.

    Parameters
    ----------
        data: dict

        version: string
            Either: v1, v2, v3, v4

    Returns
    -------
        pd.DataFrame
            Data converted to a pd.DataFrame
    """
    dfm = pd.DataFrame(data)
    if version == "v1":
        if "Date" in list(dfm.columns):
            dfm["Date"] = pd.to_datetime(dfm["Date"], unit="ms")
            dfm.set_index("Date", inplace=True)
    elif version == "v2":
        if "Date" in list(dfm.columns):
            dfm["Date"] = dfm["Date"].str[:10]
            dfm.set_index("Date", inplace=True)
            del dfm["index"]
    elif version == "v3":
        if "versionCreated" in list(dfm.columns):
            dfm["versionCreated"] = pd.to_datetime(dfm["versionCreated"], unit="ms")
            dfm.set_index("versionCreated", inplace=True)
    elif version == "v4":
        dfm = dfm.rename(
            columns={
                "price_open": "OPEN",
                "price_high": "HIGH",
                "price_low": "LOW",
                "price_close": "CLOSE",
                "volume_traded": "VOLUME",
                "time_period_start": "Date",
            }
        )
        dfm.loc[:, "Date"] = dfm.Date.apply(
            lambda x: datetime.strptime(x[:10], "%Y-%m-%d")
        )
        dfm = dfm[["Date", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]]
        dfm.set_index("Date", inplace=True)
    return dfm


def safe_concat(frames, axis=0):
    frames = [frame.loc[~frame.index.duplicated(keep="first")] for frame in frames]
    return pd.concat(frames, axis=axis)


def save_in_s3(response, bucket_name, object_name):
    too_many_requests = (
        isinstance(response, dict)
        and "data" in response
        and response["data"] is None
        and "error" in response
        and response["error"] is None
    )
    if too_many_requests:
        return TOO_MANY_REQUESTS
    eikon_not_running = (
        isinstance(response, dict)
        and "data" in response
        and response["data"] is None
        and "error" in response
        and response["error"] is not None
        and "Eikon Proxy not running or cannot be reached."
        in response["error"]["message"]
    )
    if eikon_not_running:
        return EIKON_NOT_RUNNING
    temp_dir = tempfile.TemporaryDirectory()
    path = os.path.join(temp_dir.name, object_name)
    with open(ensure_dir(path), "w") as handler:
        json.dump(response, handler)
    make_bucket_if_not_exists(bucket_name)
    print(f"Downloading {object_name}")
    put_object(path, bucket_name, object_name)
    temp_dir.cleanup()


def cache_in_s3(bucket_name, formatter):
    """
    Parameters:
    -----------
    bucket_name: string
        Where data should be saved
    """

    def decorator(func):
        """
        Parameters:
        -----------
        func: func(ric, day)
            Data downloader
        """

        def inner(ric, start_date, end_date):
            frames = []
            delta = end_date - start_date
            for i in tqdm(range(delta.days + 1)):
                day = start_date + timedelta(days=i)
                object_name = f"{ric}/{day.isoformat()[:7]}.json"
                change_of_month = day.month != (day - timedelta(days=1)).month
                first_day = day == start_date
                is_new_month = change_of_month or first_day
                if is_new_month:
                    last_month = (
                        day.month == end_date.month and day.year == end_date.year
                    )
                    this_object_exists = exists_object(bucket_name, object_name)
                    if this_object_exists and change_of_month and last_month:
                        last_modified = stat_object(
                            bucket_name, object_name
                        ).last_modified
                        already_downloaded_today = (
                            last_modified.date() == datetime.utcnow().date()
                        )
                    should_download_object = not this_object_exists or (
                        change_of_month and last_month and not already_downloaded_today
                    )
                    if should_download_object:
                        month_start_date = datetime(day.year, day.month, 1).date()
                        month_end_date = (
                            month_start_date
                            + relativedelta(months=1)
                            - timedelta(days=1)
                        )
                        response = func(ric, month_start_date, month_end_date)
                        error_message = save_in_s3(response, bucket_name, object_name)
                        if error_message is None:
                            data, _ = response["data"], response["error"]
                            dfm = formatter(data)
                        else:
                            return None, error_message
                    else:
                        data, _ = download_from_s3(bucket_name, object_name)
                        dfm = formatter(data)
                    if dfm.shape[0] > 0:
                        frames.append(dfm)
            if len(frames) == 0:
                return None, "No data"
            dfm = safe_concat(frames)
            dfm = dfm.loc[dfm.index.dropna()]
            if func.__name__ in FUNCTIONS_RETURNING_STRING_INDEX:
                dfm.index = pd.to_datetime(dfm.index, format="%Y-%m-%d")
            index = (dfm.index >= start_date) & (dfm.index <= end_date)
            dfm = dfm.loc[index, :]
            dfm.drop_duplicates(keep=False, inplace=True)
            return dfm, None

        return inner

    return decorator


def stem_to_ric(stem, suffix):
    if stem[-1] == "=":
        return stem[:-1] + suffix + "="
    return stem + suffix
