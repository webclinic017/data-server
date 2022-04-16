from datetime import datetime, timedelta
from http.client import TOO_MANY_REQUESTS
from dateutil.relativedelta import relativedelta
import json
import os
import tempfile

from ..common.data.database import download_from_s3
from ..common.data.minio_client import exists_object, make_bucket_if_not_exists, put_object, stat_object
from ..common.utils.files import ensure_dir
import pandas as pd
from tqdm import tqdm


EIKON_NOT_RUNNING = 'Eikon not running'
TOO_MANY_REQUESTS = 'Too many requests'
FUNCTIONS_RETURNING_STRING_INDEX = ['dividend__raw', 'risk_free_rate__raw']


def safe_concat(frames, axis=0):
    frames = [frame.loc[~frame.index.duplicated(keep='first')]
              for frame in frames]
    return pd.concat(frames, axis=axis)


def save_in_s3(r, bucket_name, object_name):
    too_many_requests = isinstance(r, dict) \
        and 'data' in r and r['data'] is None \
        and 'error' in r and r['error'] is None
    if too_many_requests:
        return TOO_MANY_REQUESTS
    eikon_not_running = isinstance(r, dict) \
        and 'data' in r and r['data'] is None \
        and 'error' in r and r['error'] is not None \
        and 'Eikon Proxy not running or cannot be reached.' in r['error']['message']
    if eikon_not_running:
        return EIKON_NOT_RUNNING
    temp_dir = tempfile.TemporaryDirectory()
    path = os.path.join(temp_dir.name, object_name)
    with open(ensure_dir(path), 'w') as f:
        json.dump(r, f)
    make_bucket_if_not_exists(bucket_name)
    print(f'Downloading {object_name}')
    put_object(path, bucket_name, object_name)
    temp_dir.cleanup()


def cache_in_s3(bucket_name, json_data_to_df):
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
                object_name = f'{ric}/{day.isoformat()[:7]}.json'
                change_of_month = day.month != (day - timedelta(days=1)).month
                first_day = day == start_date
                is_new_month = change_of_month or first_day
                if is_new_month:
                    last_month = day.month == end_date.month and day.year == end_date.year
                    this_object_exists = exists_object(bucket_name, object_name)
                    if this_object_exists and change_of_month and last_month:
                        last_modified = stat_object(bucket_name, object_name).last_modified
                        already_downloaded_today = last_modified.date() == datetime.utcnow().date()
                    should_download_object = \
                        not this_object_exists \
                            or (change_of_month and last_month and not already_downloaded_today)
                    if should_download_object:
                        month_start_date = datetime(day.year, day.month, 1).date()
                        month_end_date = month_start_date + \
                            relativedelta(months=1) - timedelta(days=1)
                        r = func(ric, month_start_date, month_end_date)
                        error_message = save_in_s3(r, bucket_name, object_name)
                        if error_message is None:
                            data, _ = r['data'], r['error']
                            df = json_data_to_df(data)
                        else:
                            return None, error_message
                    else:
                        data, _ = download_from_s3(bucket_name, object_name)
                        df = json_data_to_df(data)
                    if df.shape[0] > 0:
                        frames.append(df)
            if len(frames) == 0:
                return None, 'No data'
            dfm = safe_concat(frames)
            dfm = dfm.loc[dfm.index.dropna()]
            if func.__name__ in FUNCTIONS_RETURNING_STRING_INDEX:
                dfm.index = pd.to_datetime(dfm.index, format='%Y-%m-%d')
            index = (dfm.index >= start_date) & (dfm.index <= end_date)
            dfm = dfm.loc[index, :]
            dfm.drop_duplicates(keep=False,inplace=True)
            return dfm, None
        return inner
    return decorator


def stem_to_ric(stem, suffix):
    if stem[-1] == '=':
        return stem[:-1] + suffix + '='
    return stem + suffix
