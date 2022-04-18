from datetime import datetime
import os

from fastapi import FastAPI, HTTPException, Depends, Request
import numpy as np

from .fetchers.common.constants import FUTURES
from .fetchers.common.eikon import get_data
from .fetchers.factors.carry_bond import factor_carry_bond
from .fetchers.factors.carry_commodity import factor_carry_commodity
from .fetchers.factors.carry_currency import factor_carry_currency
from .fetchers.factors.carry_equity import factor_carry_equity
from .fetchers.factors.cot import factor_cot
from .fetchers.factors.currency import factor_currency
from .fetchers.factors.nav import factor_nav_long, factor_nav_short
from .fetchers.factors.roll_return import factor_roll_return
from .fetchers.factors.splits import factor_splits
from .fetchers.ohlcv import ohlcv
from .fetchers.risk_free_rate import risk_free_rate


DATA_SECRET_KEY = os.getenv("DATA_SECRET_KEY")

app = FastAPI()


def catch_errors(func):
    def decorator(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            return {"data": None, "error": str(e)}

    return decorator


def verify_token(req: Request):
    token = req.headers.get("Authorization")
    if token != DATA_SECRET_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return True


@catch_errors
def daily_factor_carry_bond(ticker: str, start_date: str, end_date: str):
    dfm, error_message = factor_carry_bond(
        future=FUTURES.get(ticker),
        start_date=datetime.strptime(start_date, "%Y-%m-%d"),
        end_date=datetime.strptime(end_date, "%Y-%m-%d"),
    )
    data = (
        dfm.reset_index()
        .replace({np.inf: np.nan})
        .replace({np.nan: None})
        .to_dict(orient="records")
        if error_message is None
        else None
    )
    return {"data": data, "error": error_message}


@app.get("/daily/factor/carry/bond")
def handler_daily_factor_carry_bond(
    ticker: str,
    start_date: str,
    end_date: str,
    authorized: bool = Depends(verify_token),
):
    return daily_factor_carry_bond(ticker, start_date, end_date)


@catch_errors
def daily_factor_carry_commodity(ticker: str, start_date: str, end_date: str):
    dfm, error_message = factor_carry_commodity(
        future=FUTURES.get(ticker),
        start_date=datetime.strptime(start_date, "%Y-%m-%d"),
        end_date=datetime.strptime(end_date, "%Y-%m-%d"),
    )
    data = (
        dfm.reset_index()
        .replace({np.inf: np.nan})
        .replace({np.nan: None})
        .to_dict(orient="records")
        if error_message is None
        else None
    )
    return {"data": data, "error": error_message}


@app.get("/daily/factor/carry/commodity")
def handler_daily_factor_carry_commodity(
    ticker: str,
    start_date: str,
    end_date: str,
    authorized: bool = Depends(verify_token),
):
    return daily_factor_carry_commodity(ticker, start_date, end_date)


@catch_errors
def daily_factor_carry_currency(ticker: str, start_date: str, end_date: str):
    dfm, error_message = factor_carry_currency(
        future=FUTURES.get(ticker),
        start_date=datetime.strptime(start_date, "%Y-%m-%d"),
        end_date=datetime.strptime(end_date, "%Y-%m-%d"),
    )
    data = (
        dfm.reset_index()
        .replace({np.inf: np.nan})
        .replace({np.nan: None})
        .to_dict(orient="records")
        if error_message is None
        else None
    )
    return {"data": data, "error": error_message}


@app.get("/daily/factor/carry/currency")
def handler_daily_factor_carry_currency(
    ticker: str,
    start_date: str,
    end_date: str,
    authorized: bool = Depends(verify_token),
):
    return daily_factor_carry_currency(ticker, start_date, end_date)


@catch_errors
def daily_factor_carry_equity(ticker: str, start_date: str, end_date: str):
    dfm, error_message = factor_carry_equity(
        future=FUTURES.get(ticker),
        start_date=datetime.strptime(start_date, "%Y-%m-%d"),
        end_date=datetime.strptime(end_date, "%Y-%m-%d"),
    )
    data = (
        dfm.reset_index()
        .replace({np.inf: np.nan})
        .replace({np.nan: None})
        .to_dict(orient="records")
        if error_message is None
        else None
    )
    return {"data": data, "error": error_message}


@app.get("/daily/factor/carry/equity")
def handler_daily_factor_carry_equity(
    ticker: str,
    start_date: str,
    end_date: str,
    authorized: bool = Depends(verify_token),
):
    return daily_factor_carry_equity(ticker, start_date, end_date)


@catch_errors
def daily_factor_cot(ticker: str, start_date: str, end_date: str):
    dfm, error_message = factor_cot(
        future=FUTURES.get(ticker),
        start_date=datetime.strptime(start_date, "%Y-%m-%d"),
        end_date=datetime.strptime(end_date, "%Y-%m-%d"),
    )
    data = (
        dfm.reset_index()
        .replace({np.inf: np.nan})
        .replace({np.nan: None})
        .to_dict(orient="records")
        if error_message is None
        else None
    )
    return {"data": data, "error": error_message}


@app.get("/daily/factor/cot")
def handler_daily_factor_cot(
    ticker: str,
    start_date: str,
    end_date: str,
    authorized: bool = Depends(verify_token),
):
    return daily_factor_cot(ticker, start_date, end_date)


@catch_errors
def daily_factor_currency(ticker: str, start_date: str, end_date: str):
    dfm, error_message = factor_currency(
        future=FUTURES.get(ticker),
        start_date=datetime.strptime(start_date, "%Y-%m-%d"),
        end_date=datetime.strptime(end_date, "%Y-%m-%d"),
    )
    data = (
        dfm.reset_index()
        .replace({np.inf: np.nan})
        .replace({np.nan: None})
        .to_dict(orient="records")
        if error_message is None
        else None
    )
    return {"data": data, "error": error_message}


@app.get("/daily/factor/currency")
def handler_daily_factor_currency(
    ticker: str,
    start_date: str,
    end_date: str,
    authorized: bool = Depends(verify_token),
):
    return daily_factor_currency(ticker, start_date, end_date)


@app.get("/daily/factor/nav/long")
def handler_daily_factor_nav_long(
    ticker: str,
    start_date: str,
    end_date: str,
    authorized: bool = Depends(verify_token),
):
    dfm, error_message = factor_nav_long(
        future=FUTURES.get(ticker),
        start_date=datetime.strptime(start_date, "%Y-%m-%d"),
        end_date=datetime.strptime(end_date, "%Y-%m-%d"),
    )
    data = (
        dfm.reset_index()
        .replace({np.inf: np.nan})
        .replace({np.nan: None})
        .to_dict(orient="records")
        if error_message is None
        else None
    )
    return {"data": data, "error": error_message}


@app.get("/daily/factor/nav/short")
def handler_daily_factor_nav_short(
    ticker: str,
    start_date: str,
    end_date: str,
    authorized: bool = Depends(verify_token),
):
    dfm, error_message = factor_nav_short(
        future=FUTURES.get(ticker),
        start_date=datetime.strptime(start_date, "%Y-%m-%d"),
        end_date=datetime.strptime(end_date, "%Y-%m-%d"),
    )
    data = (
        dfm.reset_index()
        .replace({np.inf: np.nan})
        .replace({np.nan: None})
        .to_dict(orient="records")
        if error_message is None
        else None
    )
    return {"data": data, "error": error_message}


@app.get("/daily/factor/news/headlines")
def handler_daily_factor_news_headlines(
    ticker: str,
    start_date: str,
    end_date: str,
    authorized: bool = Depends(verify_token),
):
    return {"data": None, "error": None}


@app.get("/daily/factor/news/stories")
def handler_daily_factor_news_headlines(
    ticker: str,
    start_date: str,
    end_date: str,
    authorized: bool = Depends(verify_token),
):
    return {"data": None, "error": None}


@catch_errors
def daily_factor_roll_return(ticker: str, start_date: str, end_date: str):
    dfm, error_message = factor_roll_return(
        future=FUTURES.get(ticker),
        start_date=datetime.strptime(start_date, "%Y-%m-%d"),
        end_date=datetime.strptime(end_date, "%Y-%m-%d"),
    )
    data = (
        dfm.reset_index()
        .replace({np.inf: np.nan})
        .replace({np.nan: None})
        .to_dict(orient="records")
        if error_message is None
        else None
    )
    return {"data": data, "error": error_message}


@app.get("/daily/factor/roll-return")
def handler_daily_factor_roll_return(
    ticker: str,
    start_date: str,
    end_date: str,
    authorized: bool = Depends(verify_token),
):
    return daily_factor_roll_return(ticker, start_date, end_date)


@catch_errors
def daily_factor_splits(ticker: str, start_date: str, end_date: str):
    dfm, error_message = factor_splits(
        future=FUTURES.get(ticker),
        start_date=datetime.strptime(start_date, "%Y-%m-%d"),
        end_date=datetime.strptime(end_date, "%Y-%m-%d"),
    )
    data = (
        dfm.reset_index()
        .replace({np.inf: np.nan})
        .replace({np.nan: None})
        .to_dict(orient="records")
        if error_message is None
        else None
    )
    return {"data": data, "error": error_message}


@app.get("/daily/factor/splits")
def handler_daily_splits(
    ticker: str,
    start_date: str,
    end_date: str,
    authorized: bool = Depends(verify_token),
):
    return daily_factor_splits(ticker, start_date, end_date)


@catch_errors
def daily_ohlcv(ric: str, start_date: str, end_date: str):
    dfm, error_message = ohlcv(
        ric=ric,
        start_date=datetime.strptime(start_date, "%Y-%m-%d"),
        end_date=datetime.strptime(end_date, "%Y-%m-%d"),
    )
    data = (
        dfm.reset_index()
        .replace({np.inf: np.nan})
        .replace({np.nan: None})
        .to_dict(orient="records")
        if error_message is None
        else None
    )
    return {"data": data, "error": error_message}


@app.get("/daily/ohlcv")
def handler_daily_ohlcv(
    ric: str, start_date: str, end_date: str, authorized: bool = Depends(verify_token)
):
    return daily_ohlcv(ric, start_date, end_date)


# @catch_errors
def daily_risk_free_rate(ric: str, start_date: str, end_date: str):
    dfm, error_message = risk_free_rate(
        ric=ric,
        start_date=datetime.strptime(start_date, "%Y-%m-%d"),
        end_date=datetime.strptime(end_date, "%Y-%m-%d"),
    )
    data = (
        dfm.reset_index()
        .replace({np.inf: np.nan})
        .replace({np.nan: None})
        .to_dict(orient="records")
        if error_message is None
        else None
    )
    return {"data": data, "error": error_message}


@app.get("/daily/risk-free-rate")
def handler_daily_risk_free_rate(
    ric: str, start_date: str, end_date: str, authorized: bool = Depends(verify_token)
):
    return daily_risk_free_rate(ric, start_date, end_date)


@app.get("/health")
def handler_health():
    return {"data": "OK", "error": None}


@app.get("/health/ric")
def handler_health_ric(ric: str, authorized: bool = Depends(verify_token)):
    r = get_data(instruments=[ric], fields=["TR.RIC", "CF_NAME"])
    ric_exists = False
    try:
        ric_exists = r["data"][0]["RIC"] is not None
    except:
        pass
    return {"data": ric_exists, "error": None}


@app.get("/tickers")
def handler_tickers(authorized: bool = Depends(verify_token)):
    return {"data": FUTURES, "error": None}
