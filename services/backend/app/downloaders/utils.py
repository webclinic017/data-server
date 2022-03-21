from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import json
import os
import tempfile

from common.data.database import download_from_s3
from common.data.minio_client import exists_object, make_bucket_if_not_exists, put_object, stat_object
from common.utils.files import ensure_dir
import pandas as pd
from tqdm import tqdm


def safe_concat(frames, axis=0):
    frames = [frame.loc[~frame.index.duplicated(keep='first')]
              for frame in frames]
    return pd.concat(frames, axis=axis)


def save_in_s3(bucket_name, json_data_to_df):
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
                    temp_dir = tempfile.TemporaryDirectory()
                    path = os.path.join(temp_dir.name, object_name)
                    with open(ensure_dir(path), 'w') as f:
                        json.dump(r, f)
                    make_bucket_if_not_exists(bucket_name)
                    print(f'Downloading {object_name}')
                    put_object(path, bucket_name, object_name)
                    temp_dir.cleanup()
                    data, _ = r['data'], r['error']
                    df = json_data_to_df(data)
                else:
                    data, _ = download_from_s3(bucket_name, object_name)
                    df = json_data_to_df(data)
                first_day = day == start_date
                if change_of_month or first_day:
                    frames.append(df)
            return safe_concat(frames)
        return inner
    return decorator


def stem_to_ric(stem, suffix):
    if stem[-1] == '=':
        return stem[:-1] + suffix + '='
    return stem + suffix
