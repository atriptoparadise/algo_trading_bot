import pandas as pd
from datetime import datetime, timedelta
from pandas.tseries.offsets import BDay
import numpy as np
import requests
import json
import pytz
import pickle5 as pickle
from config import *
from numba import njit
import concurrent.futures
import os


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

def check_zero_vol_mins_count(today_df, idx, open_time):
    prev = today_df.iloc[:idx]
    return (today_df.t[idx] - open_time).total_seconds() / 60.0 - len(prev)
    
def get_other_conditions(today_df, idx, v, max_vol, max_high, open_time):
    open_price = today_df.loc[0, 'o']
    curr = today_df.h[idx]
    
    price_max_ratio = round(100 * curr / max_high, 2)
    vol_max_ratio = round(100 * v / max_vol, 2)
    zero_vol_mins_count = check_zero_vol_mins_count(today_df, idx, open_time)
    
    if_curr_higher_one = int(curr > 1)
    
    if_curr_higher_open = int(curr >= open_price)
    if_curr_not_too_high = int(curr <= open_price * 1.15)
    if_curr_not_too_high_90_days = int(curr <= max_high * 1.25)
    
    return [price_max_ratio, vol_max_ratio, zero_vol_mins_count, if_curr_higher_one,
            if_curr_higher_open, if_curr_not_too_high, if_curr_not_too_high_90_days]

def strategy(today_df, max_vol, max_high, vol_=1, high_=1):
    # Trading hours only
    close_time = pd.DatetimeIndex(today_df.t)[0].replace(hour=15, minute=59, second=0, microsecond=0)
    open_time = pd.DatetimeIndex(today_df.t)[0].replace(hour=9, minute=30, second=0, microsecond=0)
    
    today_df['t'] = pd.DatetimeIndex(today_df.t)
    today_df = today_df[(today_df.t >= open_time) & (today_df.t <= close_time)].reset_index().drop(columns='index')
    
    for idx, v in enumerate(today_df.v_15m):
        if not baseline_conditions(current_v_15m=v, 
                                   max_vol=max_vol, 
                                   current_price=today_df.h[idx], 
                                   max_high=max_high, 
                                   vol_=vol_, 
                                   high_=high_):
            continue
        
        other_condi = get_other_conditions(today_df, idx, v, max_vol, max_high, open_time)
        buy_price = today_df.c[idx]
        entry_time = today_df.t[idx]
        
        # Find exit
        monitor_df = today_df[today_df.t > entry_time].reset_index().drop(columns='index')
        # Standard exit
        for i in range(len(monitor_df)):
            if monitor_df.l[i] < buy_price * 0.99 and monitor_df.h[i] > buy_price * 1.01:
                res = [np.nan, entry_time, buy_price, monitor_df.t[i], np.nan]
                res.extend(other_condi)
                return res
            
            if monitor_df.l[i] < buy_price * 0.99:
                res = [-1, entry_time, buy_price, monitor_df.t[i], buy_price * 0.99]
                res.extend(other_condi)
                return res
            
            if monitor_df.h[i] > buy_price * 1.01:
                monitor_df2 = today_df[today_df.t > monitor_df.t[i]].reset_index().drop(columns='index')
                res = stra_helper(monitor_df2, buy_price, entry_time, close_time)
                res.extend(other_condi)
                return res
            
            if monitor_df.t[i] >= close_time:
                res = [100 * (monitor_df.c[i] - buy_price) / buy_price, 
                       entry_time, buy_price, monitor_df.t[i], monitor_df.h[i]]
                res.extend(other_condi)
                return res
            
            continue
            
def stra_helper(monitor_df2, buy_price, entry_time, close_time):
    for i in range(len(monitor_df2)):
        if monitor_df2.h[i] >= buy_price * 1.03:
            return [3, entry_time, buy_price, monitor_df2.t[i], buy_price * 1.03]
        if monitor_df2.l[i] < buy_price:
            return [0, entry_time, buy_price, monitor_df2.t[i], buy_price]
        if monitor_df2.t[i] >= close_time:
            res = 100 * (monitor_df2.c[i] - buy_price) / buy_price
            return [res, entry_time, buy_price, monitor_df2.t[i], monitor_df2.h[i]]
        
        continue

@njit
def njit_baseline_conditions(current_v_15m, max_vol, current_price, max_high, vol_=1, high_=1):
    if current_v_15m >= vol_ * max_vol and current_price >= high_ * max_high:
        return True
    return False

def baseline_conditions(current_v_15m, max_vol, current_price, max_high, vol_=1, high_=1):
    if current_v_15m >= vol_ * max_vol and current_price >= high_ * max_high:
        return True
    return False

def get_moving_15m_max_volume(ticker_1_min_df):

    v_15m = [np.nan]
    v = np.array(ticker_1_min_df.v)
    rows = ticker_1_min_df.shape[0]
    for idx in range(1, rows):
        if idx < 15:
            v_15m.append(v[:idx].sum())
        else:
            v_15m.append(v[idx - 15: idx].sum())
    return v_15m

def get_today_data(ticker, date):
    response = requests.get(f'{POLY_URL}/v2/aggs/ticker/{ticker}/range/1/minute/{date}/{date}?sort=asc&apiKey={POLY_KEY}')
    res = json.loads(response.content)['results']
    today_df = pd.DataFrame(res)

    today_df['t'] = [datetime.fromtimestamp(t / 1000, 
                    tzinfo).strftime('%Y-%m-%d %H:%M:%S') \
                    for t in today_df['t']]
    today_df['v_15m'] = get_moving_15m_max_volume(today_df)
    return today_df

def backtest(saved_data, start_date, end_date, vol_=1, high_=1):
    failed_ticker = []

    results = []
    for ticker in saved_data.keys():
        # Every minute - check previous 15 min vol sum & high check
        saved_data_ticker = pd.DataFrame(saved_data[ticker])
        saved_data_ticker['datetime'] = pd.DatetimeIndex(saved_data_ticker.time).date


        days_delta = (end_date - start_date).days

        for i in range(days_delta + 1):
            date = start_date + timedelta(days=i)

            # Skip if not trading day
            if date.weekday() >= 5 or date in holidays:
                continue

            # Get past 90 days data
            past_90_df = saved_data_ticker[(saved_data_ticker.datetime >= date - timedelta(days=90)) &
                                          (saved_data_ticker.datetime < date)]

            # Get past 90 days max_vol and max_high
            max_vol = past_90_df.volume.max()
            max_high = past_90_df.high.max()

            try:
                # Get today data
                today_df = get_today_data(ticker, date)
                # If condition fits
                if not baseline_conditions(current_v_15m=today_df['v_15m'].max(), 
                                       max_vol=max_vol, 
                                       current_price=today_df['h'].max(), 
                                       max_high=max_high, 
                                       vol_=vol_, 
                                       high_=high_):
                    continue

                res = strategy(today_df, max_vol, max_high, vol_=vol_, high_=high_)
                if res:
                    res.append(ticker)
                    results.append(res)
                    print(res)

            except: 
                failed_ticker.append(ticker)
                pass

    return results, failed_ticker

def parallel_ticker_backtest(args):
    return parallel_backtest(*args)

def parallel_backtest(ticker, saved_data, start_date, end_date, vol_=1, high_=1):
    failed_ticker = []

    results = []
    # Every minute - check previous 15 min vol sum & high check
    saved_data_ticker = pd.DataFrame(saved_data[ticker])
    saved_data_ticker['datetime'] = pd.DatetimeIndex(saved_data_ticker.time).date

    days_delta = (end_date - start_date).days

    for i in range(days_delta + 1):
        date = start_date + timedelta(days=i)

        # Skip if not trading day
        if date.weekday() >= 5 or date in holidays:
            continue

        # Get past 90 days data
        past_90_df = saved_data_ticker[(saved_data_ticker.datetime >= date - timedelta(days=90)) &
                                      (saved_data_ticker.datetime < date)]

        # Get past 90 days max_vol and max_high
        max_vol = past_90_df.volume.max()
        max_high = past_90_df.high.max()

        try:
            # Get today data
            today_df = get_today_data(ticker, date)
            # If condition fits
            if not njit_baseline_conditions(current_v_15m=today_df['v_15m'].max(), 
                                       max_vol=max_vol, 
                                       current_price=today_df['h'].max(), 
                                       max_high=max_high, 
                                       vol_=vol_, 
                                       high_=high_):
                continue

            res = strategy(today_df, max_vol, max_high, vol_=vol_, high_=high_)
            if res:
                res.append(ticker)
                results.append(res)
                print(res)

        except: 
            failed_ticker.append(ticker)
            pass

    return results, failed_ticker

if __name__ == '__main__':
    
    saved_data = load_data('data')
    start_date = datetime(2022, 4, 1).date()
    end_date = datetime(2023, 3, 31).date()

    tickers = list(saved_data.keys())
    num_threads = min(len(tickers), (os.cpu_count() or 1) * 5)  # Adjust the number of threads based on your requirements

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        arguments = [(ticker, saved_data, start_date, end_date, 0.7, 0.95) for ticker in tickers]
        results_and_failed = list(executor.map(parallel_ticker_backtest, arguments))

    results = [res for res_list, _ in results_and_failed for res in res_list if res is not None]
    failed_ticker = [failed for _, failed_list in results_and_failed for failed in failed_list]