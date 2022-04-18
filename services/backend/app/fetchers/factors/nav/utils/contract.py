from datetime import date, datetime, timedelta
import tempfile

import pandas as pd
import ring

from ....common.client import Client
from ....common.constants import FUTURES, START_DATE
from ....common.minio import exists_object, fget_object


client = Client()


@ring.lru()
def get_contract(stem, day, contract_rank=0):
    chain = get_chain(stem, day)
    contract = chain.iloc[contract_rank, :]
    ltd = datetime.strptime(contract.LTD, "%Y-%m-%d").date()
    ric = contract.RIC
    if "^" in ric:
        year_3 = ric.split("^")[1]
        year_4 = ric.split("^")[0][-1]
        year_12 = "19" if year_3 in ["8", "9"] else "20"
        year = int(f"{year_12}{year_3}{year_4}")
        is_recent_ric = year >= (date.today() - timedelta(days=365)).year
        if is_recent_ric and not ric_exists(ric):
            ric = ric.split("^")[0]
    return ltd, ric


def get_front_contract(day, stem):
    future = FUTURES.get(stem, {})
    roll_offset_from_reference = timedelta(
        days=future.get("RollOffsetFromReference", -31)
    )
    reference_day = day - roll_offset_from_reference
    return get_contract(stem=stem, day=reference_day, contract_rank=0)


def get_next_contract(day, stem):
    future = FUTURES.get(stem, {})
    roll_offset_from_reference = timedelta(
        days=future.get("RollOffsetFromReference", -31)
    )
    reference_day = day - roll_offset_from_reference
    return get_contract(stem=stem, day=reference_day, contract_rank=1)


@ring.lru()
def ric_to_stem(ric):
    suffix = "^"
    stem_wo_suffix = ric.split(suffix)[0] if suffix in ric else ric
    delayed_data_prefix = "/"
    stem_wo_prefix = (
        stem_wo_suffix.split(delayed_data_prefix)[-1]
        if delayed_data_prefix in stem_wo_suffix
        else stem_wo_suffix
    )
    stem_wo_year = "".join([c for c in stem_wo_prefix if not c.isdigit()])
    stem_wo_month = stem_wo_year[:-1]
    if stem_wo_month in ["SIRT"]:
        return "SI"
    for stem in FUTURES.keys():
        if stem_wo_month == FUTURES[stem].get("Stem", {}).get("Reuters"):
            return stem


@ring.lru()
def stem_to_ric(contract_rank, day, stem):
    chain = get_chain(stem, day)
    contract = chain.iloc[contract_rank, :]
    return contract.RIC


@ring.lru()
def get_first_trade_date(ric):
    stem = ric_to_stem(ric)
    chain = get_chain(stem)
    if "^" in ric:
        contracts = chain.loc[chain.RIC == ric, "FTD"]
        if contracts.shape[0] == 0:
            return None
        first_trade_date = datetime.strptime(contracts.iloc[0], "%Y-%m-%d").date()
    else:
        index = chain.RIC.apply(lambda x: x.split("^")[0]) == ric
        contracts = chain.loc[index, :]
        if contracts.shape[0] == 0:
            return None
        ltd = min(
            contracts.LTD,
            key=lambda x: abs(datetime.strptime(x, "%Y-%m-%d").date() - date.today()),
        )
        ftd = contracts.FTD[contracts.LTD == ltd].iloc[0]
        first_trade_date = datetime.strptime(ftd, "%Y-%m-%d").date()
    return first_trade_date


@ring.lru()
def get_last_trade_date(ric):
    stem = ric_to_stem(ric)
    chain = get_chain(stem)
    if "^" in ric:
        contracts = chain.loc[chain.RIC == ric, "LTD"]
        if contracts.shape[0] == 0:
            return None
        last_trade_date = datetime.strptime(contracts.iloc[0], "%Y-%m-%d").date()
    else:
        index = chain.RIC.apply(lambda x: x.split("^")[0]) == ric
        contracts = chain.loc[index, :]
        if contracts.shape[0] == 0:
            return None
        ltd = min(
            contracts.LTD,
            key=lambda x: abs(datetime.strptime(x, "%Y-%m-%d").date() - date.today()),
        )
        last_trade_date = datetime.strptime(ltd, "%Y-%m-%d").date()
    return last_trade_date


@ring.lru()
def get_chain(stem, day=START_DATE, minimum_time_to_expiry=0):
    bucket_name = "future-expiry"
    object_name = f"{stem}.csv"
    if not exists_object(bucket_name, object_name):
        raise Exception(f"No object {bucket_name}/{object_name} in S3")
    with tempfile.NamedTemporaryFile(suffix=".csv") as f_in:
        fget_object(bucket_name, object_name, f_in.name)
        dfm = pd.read_csv(f_in.name)
    if datetime.strptime(dfm.LTD.iloc[-1], "%Y-%m-%d").date() - day < timedelta(
        days=minimum_time_to_expiry
    ):
        source = get_expiry_calendar(stem)
        raise Exception(
            f"Not enough data for {stem}. Download expiry data from {source}"
        )
    index = (dfm.LTD >= day.isoformat()) & (dfm.WeTrd == 1)  # pylint: disable=no-member
    return dfm.loc[index, :].reset_index(drop=True)  # pylint: disable=no-member


def get_expiry_calendar(stem):
    return FUTURES.get(stem, {}).get("ExpiryCalendar", "")


@ring.lru()
def ric_exists(ric):
    return client.get_health_ric(ric)


@ring.lru()
def will_expire_soon(ric, day=date.today()):
    last_trade_date = get_last_trade_date(ric)
    return day > last_trade_date - timedelta(days=10)
