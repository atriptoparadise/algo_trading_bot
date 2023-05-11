import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pandas.tseries.offsets import BDay
import json
import pickle
from twilio.rest import Client
from config import *
import logging

logfile = 'logs/signal_{}.log'.format(datetime.now().date())
logging.basicConfig(filename=logfile, level=logging.WARNING)


def load_data(filename):
    with open(f"data/{filename}.pickle", "rb") as f:
        objects = []
        while True:
            try:
                objects.append(pickle.load(f))
            except EOFError:
                break
    return objects[0]


def get_open_price(ticker, date):
    """Return today's open and high"""
    if datetime.now().hour == 9 and datetime.now().minute < 30:
        return 100000, 100000

    response = requests.get(
        f'{POLY_URL}/v2/aggs/ticker/{ticker}/range/1/day/{date}/{date}?sort=desc&apiKey={POLY_KEY}')
    content = json.loads(response.content)['results']
    return content[0]['o'], content[0]['h']


def if_exceed_high(current_price, high_list, time_list, prev_high):
    if current_price < prev_high:
        return False
    idx_high = np.argmax(np.array(high_list))
    high_time = time_list[idx_high]
    days_delta = (datetime.now() -
                  datetime.strptime(high_time, '%Y-%m-%d %H:%M:%S')).days
    if days_delta >= 20:
        return True
    return False


def get_high_so_far(ticker, date):
    response = requests.get(
        f'{POLY_URL}/v2/aggs/ticker/{ticker}/range/1/minute/{date}/{date}?sort=asc&apiKey={POLY_KEY}')
    content = json.loads(response.content)['results']
    start = datetime.now().replace(hour=13, minute=30)

    for idx, item in enumerate(content):
        time = datetime.utcfromtimestamp(item['t'] / 1000)
        if time >= start - timedelta(minutes=1):
            break

    return max([item['h'] for item in content[idx:]])


def open_high_check(ticker, open_price, current_price, date, current_to_open_ratio):
    high = get_high_so_far(ticker, date)

    if current_price <= current_to_open_ratio * open_price and high <= open_price * 1.25:
        return True
    return False


def high_current_check(current_price, open_price, high, high_to_current_ratio):
    if current_price > open_price and (high - current_price) / (current_price - open_price) <= high_to_current_ratio:
        return True, 2
    return False, 0


def nine_days_close_check(ticker, current_price, today):
    """Return True if current price is higher than close price nine business days ago"""

    nine_days = (datetime.strptime(today, '%Y-%m-%d') - BDay(9)).date()
    response = requests.get(
        f'{POLY_URL}/v1/open-close/{ticker}/{nine_days}?apiKey={POLY_KEY}')

    try:
        nine_days_close = json.loads(response.content)['close']
        if current_price >= nine_days_close:
            return True, nine_days_close
        return False
    except:
        return False


def get_bid_ask_spread_ratio(ticker):
    response = requests.get(
        f'{POLY_URL}/v3/quotes/{ticker}/?sort=asc&apiKey={POLY_KEY}')
    res = json.loads(response.content)['results']

    ask = res[0]['ask_price']
    bid = res[0]['bid_price']
    return round(100 * (ask - bid) / ((ask + bid) / 2), 3)


def get_moving_volume(ticker, date):
    """Return 15-min moving aggregated volume and last price"""

    response = requests.get(
        f'{POLY_URL}/v2/aggs/ticker/{ticker}/range/1/minute/{date}/{date}?sort=desc&apiKey={POLY_KEY}')
    content = json.loads(response.content)['results']
    last_time = content[0]['t']
    idx = 14
    if len(content) < 15:
        idx = -1
    while last_time - content[idx]['t'] > 900000:
        idx -= 1
    return sum([i['v'] for i in content[:idx + 1]]), content[0]['c']


def check_other_condi_add_signal(ticker, current_price, today, open_price, day_high, high_to_current_ratio, ticker_data, prev_high, order, volume_moving, prev_vol_max, bid_ask_spread):
    exceed_nine_days_close = nine_days_close_check(
        ticker, current_price, today)
    good, after_3pm = high_current_check(
        current_price, open_price, day_high, high_to_current_ratio)
    exceeded = if_exceed_high(
        current_price, ticker_data['high'], ticker_data['time'], prev_high)
    add_signal_to_csv(ticker, today, order, after_3pm, good, exceed_nine_days_close, exceeded,
                      volume_moving, prev_vol_max, current_price, prev_high, open_price, bid_ask_spread)


def add_signal_to_csv(ticker, today, order, after_3pm, good, exceed_nine_days_close, exceeded, volume_moving, prev_vol_max, current_price, prev_high, open_price, bid_ask_spread):
    date = datetime.strptime(today, '%Y-%m-%d').date()
    time = datetime.now().strftime("%H:%M:%S")
    weekday = int(date.weekday()) + 1

    if datetime.now().hour < 15:
        high_current_check = 'None'
    elif good:
        high_current_check = 1
    else:
        high_current_check = 0

    new_signal = [ticker, date, time, ticker + date.strftime('%Y/%m/%d') + order, order,
                  weekday, after_3pm - 1, high_current_check, exceed_nine_days_close,
                  1 if exceeded else 0, volume_moving, prev_vol_max,
                  volume_moving / prev_vol_max, current_price, prev_high,
                  open_price, (current_price / open_price - 1) * 100,
                  current_price * volume_moving, 1 if current_price *
                  volume_moving >= 20000000 else 0,
                  bid_ask_spread]

    new = pd.Series(new_signal, index=['symbol', 'date', 'time', 'symbol_date', 'order', 'weekday',
                                       'after_3_pm', 'high_current_or_close_check', 'nine_days_close_check',
                                       'if_exceed_previous_high', 'moving_volume', 'previous_volume_max',
                                       'volume_ratio', 'entry_price', 'previous_high', 'open_price',
                                       'open_ratio', 'amount', 'if_larger_20m', 'bid_ask_spread'])

    df = pd.read_csv('data/signals.csv', index_col=0)
    if new[3] in df.symbol_date.unique():
        return
    df = df.append(new, ignore_index=True)
    df.to_csv('data/signals.csv')


def send_signal_text(text, to_number=['+16467156606', '+19174975345', '+15713520589']):
    account_sid = TW_ACCOUNT_SID
    auth_token = TW_AUTH_TOKEN
    from_number = TW_NUMBER

    client = Client(account_sid, auth_token)

    for number in to_number:
        client.messages.create(
            body=text,
            from_=from_number,
            to=number
        )


def log_print_text(ticker, current_price, prev_high, volume_moving, prev_vol_max, bid_ask_spread, send_text=True, signal_type='Signal 1'):
    log_text = f'{ticker} {signal_type}, price: {current_price}, price%: {round(current_price / prev_high, 2)}, vol%: {round(volume_moving / prev_vol_max, 2)}, bid ask spread: {bid_ask_spread} @ {datetime.now()}'

    print(log_text)
    logging.warning(log_text)

    df = pd.read_csv('data/signals.csv', index_col=0)
    ticker_date = ticker + datetime.today().strftime('%Y/%m/%d') + signal_type
    
    if send_text and ticker_date not in df.symbol_date.unique():
        send_signal_text(text=log_text)