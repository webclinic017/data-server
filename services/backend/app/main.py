from datetime import datetime
from distutils.log import error
import os

from fastapi import FastAPI, HTTPException, Depends, Request

from common.data.constants import FUTURES
import numpy as np
from .downloaders.factor_carry_bond import factor_carry_bond
from .downloaders.factor_carry_commodity import factor_carry_commodity
from .downloaders.factor_carry_currency import factor_carry_currency
from .downloaders.factor_carry_equity import factor_carry_equity
from .downloaders.factor_cot import factor_cot
from .downloaders.factor_currency import factor_currency
from .downloaders.factor_roll_return import factor_roll_return
from .downloaders.nav import nav_long, nav_short
from .downloaders.ohlcv import ohlcv
from .downloaders.splits import splits


DATA_SECRET_KEY = os.getenv('DATA_SECRET_KEY')

app = FastAPI()


def catch_errors(func):
    def decorator(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            return {'data': None, 'error': str(e)}
    return decorator


def verify_token(req: Request):
    token = req.headers.get('Authorization')
    if token != DATA_SECRET_KEY:
        raise HTTPException(
            status_code=401,
            detail='Unauthorized'
        )
    return True


@catch_errors
def factor_carry_bond(ticker:str, start_date:str, end_date:str):
    dfm, error_message = factor_carry_bond(
        future=FUTURES.get(ticker),
        start_date=datetime.strptime(start_date, '%Y-%m-%d'),
        end_date=datetime.strptime(end_date, '%Y-%m-%d'))
    data = dfm.reset_index().replace({np.nan: None}).to_dict(orient='records') \
        if error_message is None else None
    return { 'data': data, 'error': error_message }


@app.get('/daily/factor/carry/bond')
def handler_daily_factor_carry_bond(ticker:str, start_date:str, end_date:str, 
        authorized:bool = Depends(verify_token)):
    return factor_carry_bond(ticker, start_date, end_date)
    

@catch_errors
def daily_factor_carry_commodity(ticker:str, start_date:str, end_date:str):
    dfm, error_message = factor_carry_commodity(
        future=FUTURES.get(ticker),
        start_date=datetime.strptime(start_date, '%Y-%m-%d'),
        end_date=datetime.strptime(end_date, '%Y-%m-%d'))
    data = dfm.reset_index().replace({np.nan: None}).to_dict(orient='records') \
        if error_message is None else None
    return { 'data': data, 'error': error_message }


@app.get('/daily/factor/carry/commodity')
def handler_daily_factor_carry_commodity(ticker:str, start_date:str, end_date:str, 
        authorized:bool = Depends(verify_token)):
    return daily_factor_carry_commodity(ticker, start_date, end_date)


@catch_errors
def daily_factor_carry_currency(ticker:str, start_date:str, end_date:str):
    dfm, error_message = factor_carry_currency(
        future=FUTURES.get(ticker),
        start_date=datetime.strptime(start_date, '%Y-%m-%d'),
        end_date=datetime.strptime(end_date, '%Y-%m-%d'))
    data = dfm.reset_index().replace({np.nan: None}).to_dict(orient='records') \
        if error_message is None else None
    return { 'data': data, 'error': error_message }


@app.get('/daily/factor/carry/currency')
def handler_daily_factor_carry_currency(ticker:str, start_date:str, end_date:str, 
        authorized:bool = Depends(verify_token)):
    return daily_factor_carry_currency(ticker, start_date, end_date)


@catch_errors
def daily_factor_carry_equity(ticker:str, start_date:str, end_date:str):
    dfm, error_message = factor_carry_equity(
        future=FUTURES.get(ticker),
        start_date=datetime.strptime(start_date, '%Y-%m-%d'),
        end_date=datetime.strptime(end_date, '%Y-%m-%d'))
    data = dfm.reset_index().replace({np.nan: None}).to_dict(orient='records') \
        if error_message is None else None
    return { 'data': data, 'error': error_message }


@app.get('/daily/factor/carry/equity')
def handler_daily_factor_carry_equity(ticker:str, start_date:str, end_date:str, 
        authorized:bool = Depends(verify_token)):
    return daily_factor_carry_equity(ticker, start_date, end_date)


@catch_errors
def daily_factor_cot(ticker:str, start_date:str, end_date:str):
    dfm, error_message = factor_cot(
        future=FUTURES.get(ticker),
        start_date=datetime.strptime(start_date, '%Y-%m-%d'),
        end_date=datetime.strptime(end_date, '%Y-%m-%d'))
    data = dfm.reset_index().replace({np.nan: None}).to_dict(orient='records') \
        if error_message is None else None
    return { 'data': data, 'error': error_message }


@app.get('/daily/factor/cot')
def handler_daily_factor_cot(ticker:str, start_date:str, end_date:str, 
        authorized:bool = Depends(verify_token)):
    return daily_factor_cot(ticker, start_date, end_date)


@catch_errors
def daily_factor_currency(ticker:str, start_date:str, end_date:str):
    dfm, error_message = factor_currency(
        future=FUTURES.get(ticker),
        start_date=datetime.strptime(start_date, '%Y-%m-%d'),
        end_date=datetime.strptime(end_date, '%Y-%m-%d'))
    dfm.reset_index(inplace=True)
    data = dfm.reset_index().replace({np.nan: None}).to_dict(orient='records') \
        if error_message is None else None
    return { 'data': data, 'error': error_message }


@app.get('/daily/factor/currency')
def handler_daily_factor_currency(ticker:str, start_date:str, end_date:str, authorized:bool = Depends(verify_token)):
    return daily_factor_currency(ticker, start_date, end_date)


@catch_errors
def daily_factor_roll_return(ticker:str, start_date:str, end_date:str):
    dfm, error_message = factor_roll_return(
        future=FUTURES.get(ticker),
        start_date=datetime.strptime(start_date, '%Y-%m-%d'),
        end_date=datetime.strptime(end_date, '%Y-%m-%d'))
    dfm.reset_index(inplace=True)
    data = dfm.reset_index().replace({np.nan: None}).to_dict(orient='records') \
        if error_message is None else None
    return { 'data': data, 'error': error_message }


@app.get('/daily/factor/roll-return')
def handler_daily_factor_roll_return(ticker:str, start_date:str, end_date:str, 
        authorized:bool = Depends(verify_token)):
    return daily_factor_roll_return(ticker, start_date, end_date)


@app.get('/daily/nav/long')
def handler_daily_nav_long(ticker:str, start_date:str, end_date:str, 
        authorized:bool = Depends(verify_token)):
    dfm, error_message = nav_long(
        future=FUTURES.get(ticker),
        start_date=datetime.strptime(start_date, '%Y-%m-%d'),
        end_date=datetime.strptime(end_date, '%Y-%m-%d'))
    dfm.reset_index(inplace=True)
    data = dfm.reset_index().replace({np.nan: None}).to_dict(orient='records') \
        if error_message is None else None
    return { 'data': data, 'error': error_message }


@app.get('/daily/nav/short')
def handler_daily_nav_short(ticker:str, start_date:str, end_date:str, 
        authorized:bool = Depends(verify_token)):
    dfm, error_message = nav_short(
        future=FUTURES.get(ticker),
        start_date=datetime.strptime(start_date, '%Y-%m-%d'),
        end_date=datetime.strptime(end_date, '%Y-%m-%d'))
    dfm.reset_index(inplace=True)
    data = dfm.reset_index().replace({np.nan: None}).to_dict(orient='records') \
        if error_message is None else None
    return { 'data': data, 'error': error_message }


@catch_errors
def daily_ohlcv(ticker:str, start_date:str, end_date:str):
    dfm, error_message = ohlcv(
        future=FUTURES.get(ticker),
        start_date=datetime.strptime(start_date, '%Y-%m-%d'),
        end_date=datetime.strptime(end_date, '%Y-%m-%d'))
    data = dfm.reset_index().replace({np.nan: None}).to_dict(orient='records') \
        if error_message is None else None
    return { 'data': data, 'error': error_message }

@app.get('/daily/ohlcv')
def handler_daily_ohlcv(ticker:str, start_date:str, end_date:str, authorized:bool = Depends(verify_token)):
    return daily_ohlcv(ticker, start_date, end_date)


@catch_errors
def daily_splits(ticker:str, start_date:str, end_date:str):
    dfm, error_message = splits(
        future=FUTURES.get(ticker),
        start_date=datetime.strptime(start_date, '%Y-%m-%d'),
        end_date=datetime.strptime(end_date, '%Y-%m-%d'))
    data = dfm.reset_index().replace({np.nan: None}).to_dict(orient='records') \
        if error_message is None else None
    return { 'data': data, 'error': error_message }


@app.get('/daily/splits')
def handler_daily_splits(ticker:str, start_date:str, end_date:str, authorized:bool = Depends(verify_token)):
    return daily_splits(ticker, start_date, end_date)


@app.get('/health')
def handler_health():
    return { 'data': 'OK', 'error': None }


@app.get('/tickers')
def handler_tickers(authorized:bool = Depends(verify_token)):
    return { 'data': FUTURES, 'error': None }


