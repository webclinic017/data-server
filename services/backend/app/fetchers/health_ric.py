from datetime import date, datetime, timedelta

from .common.cache import download_from_s3, save_in_s3
from .common.eikon import get_data
from .common.minio import (
    exists_object,
    stat_object,
)


def cache_in_s3(bucket_name):
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

        def inner(ric):
            is_old_ric = False
            if "^" in ric:
                year_3 = ric.split("^")[1]
                year_4 = ric.split("^")[0][-1]
                year_12 = "19" if year_3 in ["8", "9"] else "20"
                year = int(f"{year_12}{year_3}{year_4}")
                is_old_ric = year < (date.today() - timedelta(days=365)).year
            object_name = f"{ric}.json"
            this_object_exists = exists_object(bucket_name, object_name)
            if this_object_exists and not is_old_ric:
                last_modified = stat_object(bucket_name, object_name).last_modified
                already_downloaded_today = (
                    last_modified.date() == datetime.utcnow().date()
                )
            should_download_object = not this_object_exists or (
                not is_old_ric and not already_downloaded_today
            )
            if should_download_object:
                response = func(ric)
                error_message = save_in_s3(response, bucket_name, object_name)
                if error_message is None:
                    data, _ = response["data"], response["error"]
                else:
                    return None, error_message
            else:
                data, _ = download_from_s3(bucket_name, object_name)
            return data, None

        return inner

    return decorator


@cache_in_s3("health-ric")
def risk_free_rate__raw(ric: str):
    return get_data(instruments=[ric], fields=["TR.RIC", "CF_NAME"])


def health_ric(ric: str):
    response, error_message = risk_free_rate__raw(ric)
    ric_exists = False
    if error_message is None:
        ric_exists = len(response) == 1 and response[0]["RIC"] is not None
    return {"data": ric_exists, "error": None}
