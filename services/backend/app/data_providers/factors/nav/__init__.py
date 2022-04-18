import pandas as pd

from .strategies.buy_and_hold import BuyAndHoldBacktester
from .strategies.sell_and_hold import SellAndHoldBacktester


def factor_nav_long(future, start_date, end_date):
    stem = future["Stem"]["Reuters"]
    stems = [stem]
    cash = 1000000
    leverage = 0.1
    parameters = {"number_of_positions": len(stems)}
    backtester = BuyAndHoldBacktester(
        stems,
        start_date.date(),
        end_date.date(),
        cash,
        leverage,
        parameters,
        no_check=True,
        plot=False,
        suffix="long",
    )
    backtester.run()
    dfm = pd.DataFrame(data=backtester.data, index=backtester.dates)
    arrays = [dfm.index, [stem] * len(dfm)]
    tuples = list(zip(*arrays))
    dfm.index = pd.MultiIndex.from_tuples(tuples, names=["Date", "Stem"])
    dfm = dfm.rename(columns={"Nav": "NavLong"})
    return dfm, None


def factor_nav_short(future, start_date, end_date):
    stem = future["Stem"]["Reuters"]
    stems = [stem]
    cash = 1000000
    leverage = 0.1
    parameters = {"number_of_positions": len(stems)}
    backtester = SellAndHoldBacktester(
        stems,
        start_date.date(),
        end_date.date(),
        cash,
        leverage,
        parameters,
        no_check=True,
        plot=False,
        suffix="short",
    )
    backtester.run()
    dfm = pd.DataFrame(data=backtester.data, index=backtester.dates)
    arrays = [dfm.index, [stem] * len(dfm)]
    tuples = list(zip(*arrays))
    dfm.index = pd.MultiIndex.from_tuples(tuples, names=["Date", "Stem"])
    dfm = dfm.rename(columns={"Nav": "NavShort"})
    return dfm, None
