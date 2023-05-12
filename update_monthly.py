import math
import statsmodels.api as sm
from joblib import Parallel, delayed
import pandas as pd
import numpy as np
import requests
import json
import pytz
from datetime import datetime
import pickle5 as pickle
from config import *


tzinfo = pytz.timezone("America/New_York")


def load_data(filename):
	with open(f"data/{filename}.pickle", "rb") as f:
		objects = []
		while True:
			try:
				objects.append(pickle.load(f))
			except EOFError:
				break
	return objects[0]

def save_data(filename, dic_name):
	with open(f"data/{filename}.pickle", 'wb') as f:
		pickle.dump(dic_name, f, pickle.HIGHEST_PROTOCOL)
                
def get_day_close(ticker, start, end):
    response = requests.get(
        f'{POLY_URL}/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}?sort=asc&apiKey={POLY_KEY}')
    content = json.loads(response.content)['results']
    df = pd.DataFrame(content)
    df['t'] = [datetime.fromtimestamp(t / 1000, 
                    tzinfo).strftime('%Y-%m-%d %H:%M:%S') \
                    for t in df['t']]
    df = df[['c', 't']]
    df['return'] = df.c.pct_change()
    df.columns = ['close', 'datetime', 'return']
    return df

def get_spy_return(start, end):
    ticker = 'SPY'
    response = requests.get(
            f'{POLY_URL}/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}?sort=asc&apiKey={POLY_KEY}')
    content = json.loads(response.content)['results']
    
    spy = pd.DataFrame(content)
    spy['t'] = [datetime.fromtimestamp(t / 1000, 
                        tzinfo).strftime('%Y-%m-%d %H:%M:%S') \
                        for t in spy['t']]

    spy = spy[['c', 't']]
    spy.columns = ['spy_close', 'datetime']
    spy['spy_return'] = spy.spy_close.pct_change()
    return spy

def get_beta_ticker(spy, ticker, start, end):
    """Same day as SPY"""
    try:
        ticker_df = get_day_close(ticker, start, end)
        df = spy.merge(ticker_df, on='datetime', how='left')
        df = df.dropna()
        X = df['spy_return']
        y = df['return']
        X1 = sm.add_constant(X)
    
        # Conduct the regression
        model = sm.OLS(y, X1)
        results = model.fit()

        beta = results.params['spy_return']
        if beta <= 0.5:
            beta_log2 = -1
        else:
            beta_log2 = round(math.log2(beta), 2)
        return round(beta, 2), beta_log2
    except:
        return np.nan, np.nan
    
def get_mkt_cap(ticker, end):
    response = requests.get(
        f'{POLY_URL}/v3/reference/tickers/{ticker}?date={end}&apiKey={POLY_KEY}')
    content = json.loads(response.content)
    try:
        mkt_cap = content['results']['market_cap']
        return mkt_cap, large_number_formatter(mkt_cap)
    except:
        return np.nan, np.nan

def large_number_formatter(x):
    decades = [1e12, 1e9, 1e6, 1e3]
    suffix = ['T', 'B', 'M', 'K']
    if np.abs(x) < 1000:
        return '{0:.2f}'.format(x)
    else:
        for i, d in enumerate(decades):
            if np.abs(x) >= d:
                val = x / float(d)
                signf = len(str(val).split('.')[1])
                if signf == 0:
                    return '{val:d}{suffix}'.format(val=int(val), suffix=suffix[i])
                else:
                    if signf == 1:
                        if str(val).split('.')[1] == '0':
                            return '{val:d}{suffix}'.format(val=int(round(val)), suffix=suffix[i])
                    tx = '{'+'val:.2f'.format(signf=signf) + '}{suffix}'
                    return tx.format(val=val, suffix=suffix[i])
        return x

def process_data(k, v, spy, start, end):
    beta, trailing = get_beta_ticker(spy, k, start, end)
    mkt_cap, mkt_cap_string = get_mkt_cap(k, end)
    v['beta'] = beta
    v['beta_trailing_perc'] = trailing
    v['mkt_cap'] = mkt_cap
    v['mkt_cap_string'] = mkt_cap_string
    print([k, beta, trailing, mkt_cap, mkt_cap_string])
    return k, v


if __name__ == "__main__":
    data = load_data('data')
    start = datetime(2022, 5, 1).date()
    end = datetime(2023, 5, 1).date()
    spy = get_spy_return(start, end)

    results = Parallel(n_jobs=1)(delayed(process_data)(k, v, spy, start, end) for k, v in data.items())

    for k, v in results:
        data[k] = v
        
    save_data('data', data)
