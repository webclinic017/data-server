from datetime import date, datetime, timedelta

import pandas as pd
import ring
import tempfile
import time
from tqdm import tqdm

from ....common.client import Client
from ....common.constants import FUTURES, LETTERS, START_DATE
from ....common.eikon import get_timeseries
from ....common.minio import exists_object, fget_object


client = Client()


@ring.lru()
def get_contract(stem, day, contract_rank=0):
    chain = get_chain(stem, day)
    contract = chain.iloc[contract_rank, :]
    ltd = datetime.strptime(contract.LTD, "%Y-%m-%d").date()
    ric = contract.RIC if ric_exists(contract.RIC) else contract.RIC.split("^")[0]
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


def list_existing_maturities(stem, start_date, end_date):
    data_dict = {}
    err = {}
    delta = end_date - start_date

    datas = {}
    for i in tqdm(range(delta.days + 1)):
        day = start_date + timedelta(days=i)
        if LETTERS[day.month - 1] not in FUTURES[stem].get("NormalMonths", []):
            continue
        ric = to_outright(stem, day.year, day.month, is_active=False)
        if ric not in datas:
            r = get_timeseries(
                rics=ric,
                fields=["OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"],
                start_date=start_date.isoformat(),
                end_date=end_date.isoformat(),
                interval="daily",
            )
            datas[ric] = r["data"]
            time.sleep(1)
        data = datas[ric]
        if data is None or len(data) == 0:
            if ric not in err:
                err[ric] = day.isoformat()
            continue
        df = pd.DataFrame(data)
        df["Date"] = df["Date"].apply(lambda x: pd.to_datetime(x, unit="ms"))
        df.set_index("Date", inplace=True)
        if ric not in data_dict:
            data_dict[ric] = {
                "YearMonth": df.index[-1].date().strftime("%Y-%m"),
                "FND": None,
                "LTD": df.index[-1].date().isoformat(),
                "RIC": ric,
                "WeTrd": 1,
            }
    data = list(data_dict.values())
    return data, err


def to_short_maturity(maturity):
    """Convert a long maturity (example M24) into a short maturity (M4).

    :param maturity: Maturity to be converted
    :return: Returns a short maturity
    """
    return "{}{}".format(maturity[0], maturity[2])


def stem_to_ric_from_year(stem, year, month, is_active):
    letter = LETTERS[month - 1]
    maturity = f"{letter}{(year % 100):02d}"
    formatted_maturity = to_short_maturity(maturity)
    reuters_stem = FUTURES[stem]["Stem"]["Reuters"]
    ric = f"{reuters_stem}{formatted_maturity}"
    if not is_active:
        return "{}^{}".format(ric, maturity[1])
    return ric


@ring.lru()
def to_outright(stem, year, month, day=None, is_active=False):
    if day is None:
        return stem_to_ric_from_year(stem, year, month, is_active)
    if date.today() > date(year, month, day) + timedelta(days=10):
        return stem_to_ric_from_year(stem, year, month, is_active=False)
    ric = stem_to_ric_from_year(stem, year, month, is_active=False)
    if ric_exists(ric):
        return ric
    return stem_to_ric_from_year(stem, year, month, is_active=True)


@ring.lru()
def ric_to_stem(ric):
    if not is_outright(ric):
        ric = get_outright_ric(ric)
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


def get_outright_ric(ric):
    if "-" not in ric:
        return ric
    start_index = 1 if ric[0].isdigit() else 0
    outright_ric = ric.split("-")[0][start_index:]
    if "^" in ric:
        outright_ric += "^" + ric.split("^")[1]
    if outright_ric.startswith("SIRT"):
        outright_ric = outright_ric[:2] + outright_ric[4:]
    return outright_ric


def is_outright(ric):
    return "-" not in ric


@ring.lru()
def get_last_trade_date(ric):
    ric_outright = get_outright_ric(ric)
    stem = ric_to_stem(ric_outright)
    chain = get_chain(stem)
    if "^" in ric_outright:
        contracts = chain.loc[chain.RIC == ric_outright, "LTD"]
        if contracts.shape[0] == 0:
            return None
        last_trade_date = datetime.strptime(contracts.iloc[0], "%Y-%m-%d").date()
    else:
        index = chain.RIC.apply(lambda x: x.split("^")[0]) == ric_outright
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
        df = pd.read_csv(f_in.name)
    if datetime.strptime(df.LTD.iloc[-1], "%Y-%m-%d").date() - day < timedelta(
        days=minimum_time_to_expiry
    ):
        source = get_expiry_calendar(stem)
        raise Exception(
            f"Not enough data for {stem}. Download expiry data from {source}"
        )
    index = (df.LTD >= day.isoformat()) & (df.WeTrd == 1)
    return df.loc[index, :].reset_index(drop=True)


def get_expiry_calendar(stem):
    return FUTURES.get(stem, {}).get("ExpiryCalendar", "")


def get_start_date(stem):
    start_date = FUTURES.get(stem, {}).get("StartDate", "1980-01-01")
    return datetime.strptime(start_date, "%Y-%m-%d").date()


@ring.lru()
def ric_exists(ric):
    return client.get_health_ric(ric)


@ring.lru()
def get_is_active(ric, last_trade_date=None, day=date.today()):
    if last_trade_date is None:
        last_trade_date = get_last_trade_date(ric)
    if last_trade_date >= day:
        return True
    if last_trade_date < day - timedelta(days=30):
        return False
    return not ric_exists(ric)


@ring.lru()
def get_ric_and_stem(day, contract_rank=0, ric=None, stem=None):
    ric = ric if ric else stem_to_ric(contract_rank, day, stem)
    stem = stem if stem else ric_to_stem(ric)
    chain = get_chain(stem)
    contract = chain.loc[chain.RIC == ric, :]
    if contract.shape[0] == 1:
        last_trade_date = datetime.strptime(contract.LTD.values[0], "%Y-%m-%d").date()
        is_active = get_is_active(ric, last_trade_date, date.today())
        ric = ric.split("^")[0] if is_active else ric
    return ric, stem


@ring.lru()
def will_expire_soon(ric, day=date.today()):
    last_trade_date = get_last_trade_date(ric)
    return day > last_trade_date - timedelta(days=10)
