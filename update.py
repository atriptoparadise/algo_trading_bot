import numpy as np
import pandas as pd
import pickle
import requests, json
from datetime import datetime, timedelta
from config import *
from joblib import Parallel, delayed


holidays = [datetime(2022, 4, 15).date(),
            datetime(2022, 5, 30).date(),
            datetime(2022, 6, 20).date(),
            datetime(2022, 7, 4).date(),
            datetime(2022, 9, 5).date(),
            datetime(2022, 11, 24).date(),
            datetime(2022, 12, 26).date(),
            datetime(2023, 1, 2).date(),
            datetime(2023, 1, 16).date(),
            datetime(2023, 2, 20).date(),
           ]

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

def get_moving_15m_max_volume(res):
    ticker_1_min_df = pd.DataFrame(res)

    v_15m = []
    v = np.array(ticker_1_min_df.v)
    rows = ticker_1_min_df.shape[0]
    for idx in range(1, rows):
        if idx < 15:
            v_15m.append(v[:idx].sum())
        else:
            v_15m.append(v[idx - 15: idx].sum())
    return np.max(v_15m)

def get_data(ticker, start_date, end_date):
    # Get data in one min bar
    response = requests.get(f'{POLY_URL}/v2/aggs/ticker/{ticker}/range/1/minute/{start_date}/{end_date}?sort=asc&apiKey={POLY_KEY}')
    res = json.loads(response.content)['results']

    # Get moving 15m max volume
    max_volume = get_moving_15m_max_volume(res)
    # Get high and its time
    max_high =  max([item['h'] for item in res])
    idx_high = np.argmax([item['h'] for item in res])
    high_time = (datetime.utcfromtimestamp(res[idx_high]['t'] / 1000) - timedelta(hours=4)).strftime('%Y-%m-%d %H:%M:%S')

    ticker_data = {'volume': [max_volume], 'high': [max_high], 'time': [high_time]}
    return ticker_data

def init_data(ticker, data, start_date, end_date):
    days_delta = (end_date - start_date).days
    data[ticker] = {'volume': [], 'high': [], 'time': [], 'date': None}

    for i in range(days_delta + 1):
        date = start_date + timedelta(days=i)
        if date.weekday() >= 5 or date in holidays:
            continue
        try:
            res = get_data(ticker, date, date)
            data[ticker]['volume'].append(res['volume'][0])
            data[ticker]['high'].append(res['high'][0])
            data[ticker]['time'].append(res['time'][0])
        except Exception as e: 
            # print(ticker, date)
            # print(e)
            pass
    
    while end_date.weekday() >= 5:
        end_date = end_date - timedelta(days=1)

    if data[ticker]['volume']:
        data[ticker]['date'] = end_date
        print(f'new ticker {ticker} is updated')
    else:
        print(f'{ticker} is empty')
    

def update_ticker(ticker, today, last_updated_date, saved_data):
    while today.weekday() >= 5:
        today = today - timedelta(days=1)
    
    if today == last_updated_date:
        print(f'{ticker} is already up-to-date')
        return
    
    days_delta = (today - last_updated_date).days
    for i in range(days_delta):
        date = last_updated_date + timedelta(i + 1)
        if date.weekday() >= 5:
            continue
        try:
            res = get_data(ticker, date, date)
            saved_data[ticker]['volume'].append(res['volume'][0])
            saved_data[ticker]['high'].append(res['high'][0])
            saved_data[ticker]['time'].append(res['time'][0])
            saved_data[ticker]['date'] = date
        except:
            print(ticker, date)
            pass
    
    remove_old_data(saved_data, ticker, 90)

def remove_old_data(saved_data, ticker, days):
    today = datetime.now()
    count = 0

    for time in saved_data[ticker]['time']:
        if (today - datetime.strptime(time, '%Y-%m-%d %H:%M:%S')).days > days:
            count += 1

    if count == 0:
        return
    saved_data[ticker]['volume'] = saved_data[ticker]['volume'][count:]
    saved_data[ticker]['high'] = saved_data[ticker]['high'][count:]
    saved_data[ticker]['time'] = saved_data[ticker]['time'][count:]

def init_data_wrapper(args):
    return init_data(*args)

def update_ticker_wrapper(args):
    return update_ticker(*args)

def run(ticker_list=None, start_date=None, end_date=None):
    """
    end_date is required.
    start_date is required when initial the data.
    """

    saved_data = load_data('data')
    saved_list = saved_data.keys()
    
    if ticker_list is None:
        args_list = [(ticker, end_date, saved_data[ticker]['date'], saved_data) for ticker in saved_list]
        Parallel(n_jobs=-1)(delayed(update_ticker_wrapper)(args) for args in args_list)
    else:
        new_tickers = [ticker for ticker in ticker_list if ticker not in saved_list]
        args_list = [(ticker, saved_data, start_date, end_date) for ticker in new_tickers]
        Parallel(n_jobs=-1)(delayed(init_data_wrapper)(args) for args in args_list)

        existing_tickers = [ticker for ticker in ticker_list if ticker in saved_list]
        args_list = [(ticker, end_date, saved_data[ticker]['date'], saved_data) for ticker in existing_tickers]
        Parallel(n_jobs=-1)(delayed(update_ticker_wrapper)(args) for args in args_list)

    save_data('data', saved_data)

if __name__ == "__main__":	
    start_date = start = datetime(2023, 2, 1).date()
    end_date = start = datetime(2023, 5, 2).date()

    ticker_df = pd.read_csv('data/stocks_list_202304.csv')
    ticker_list = ticker_df.Symbol.unique()
    run(ticker_list=ticker_list, start_date=start_date, end_date=end_date)