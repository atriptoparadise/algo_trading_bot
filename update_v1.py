import alpaca_trade_api as tradeapi
import pandas as pd 
import numpy as np
import pickle
import requests, json
from datetime import datetime, timedelta
from config import *

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

def get_data(ticker, start_date, end_date):
    response = requests.get(f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/15/minute/{start_date}/{end_date}?sort=asc&apiKey={API_KEY}')
    res = json.loads(response.content)['results']
    max_volume, max_high = max([item['v'] for item in res]), max([item['h'] for item in res])
    idx_high = np.argmax([item['h'] for item in res])
    unix_time = res[idx_high]['t'] / 1000
    high_time = (datetime.utcfromtimestamp(unix_time) - timedelta(hours=4)).strftime('%Y-%m-%d %H:%M:%S')
    ticker_data = {'volume': [max_volume], 'high': [max_high], 'time': [high_time]}
    return ticker_data

def init_data(ticker, data, midnight):
    monday = [datetime(2020, 8, 31).date(), datetime(2020, 9, 7).date(), 
                datetime(2020, 9, 14).date(), datetime(2020, 9, 21).date(),
                datetime(2020, 9, 28).date(), datetime(2020, 10, 5).date(),
                datetime(2020, 10, 12).date(), datetime(2020, 10, 19).date(),
                datetime(2020, 10, 26).date()]

    data[ticker] = {'volume': [], 'high': [], 'time': [], 'date': None}
    for start_date in monday:
        try:
            res = get_data(ticker, start_date, start_date + timedelta(days=4))
            data[ticker]['volume'].append(res['volume'][0])
            data[ticker]['high'].append(res['high'][0])
            data[ticker]['time'].append(res['time'][0])
        except:
            pass
    
    date = datetime.now().date()
    while date.weekday() >= 5:
        date = date - timedelta(days=1)
    data[ticker]['date'] = date

    return data

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
            pass

def run(ticker_list=None, midnight=False):
    saved_data = load_data('data_new')
    saved_list = saved_data.keys()

    if not ticker_list:
        ticker_list = saved_list

    today = datetime.now().date()
    if midnight:
        today = today - timedelta(days=1)
    
    for ticker in ticker_list:
        if ticker not in saved_list:
            init_data(ticker, saved_data, midnight)
        else:
            if today == saved_data[ticker]['date']:
                print(f'{ticker} is already up-to-date')
            else:
                update_ticker(ticker, today, saved_data[ticker]['date'], saved_data)
                print(f'{ticker} updated')
    
    save_data('data_new', saved_data)


# ticker_list = load_data('watch_list')
# data = load_data('data_new')

# data = init_data(ticker_list, data)
# data = temp(ticker_list, data)
# save_data('data_new', data)
# print(data['AAPL'])
run(['NIO'])