from common.data.cot import get_commitment_of_traders
import pandas as pd


def factor_cot(future, start_date, end_date):
    stem = future['Stem']['Reuters']
    dfm = get_commitment_of_traders(stem, cot_type='F')
    columns = ['Money Manager Longs', 'Money Manager Shorts', 'Money Manager Spreads',
               'Non Reportable Longs', 'Non Reportable Shorts', 'Open Interest',
               'Other Reportable Longs', 'Other Reportable Shorts',
               'Other Reportable Spreads', 'Producer/Merchant/Processor/User Longs',
               'Producer/Merchant/Processor/User Shorts', 'Swap Dealer Longs',
               'Swap Dealer Shorts', 'Swap Dealer Spreads', 'Total Reportable Longs',
               'Total Reportable Shorts']
    dfm = dfm[columns]
    arrays = [dfm.index, [stem] * len(dfm)]
    tuples = list(zip(*arrays))
    dfm.index = pd.MultiIndex.from_tuples(tuples, names=['Date', 'Stem'])
    return dfm
