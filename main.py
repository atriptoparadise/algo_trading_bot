import alpaca_trade_api as tradeapi
import pickle
import time as t
from datetime import datetime
import schedule
import logging
import requests
import json

logfile = 'logs/signal_{}.log'.format(datetime.now().date())
logging.basicConfig(filename=logfile, level=logging.WARNING)

def setup():
    api = tradeapi.REST('PKDLZFEJND51O3Q3TZ3F', 
                        'xguRqEqr3cw0sNA2cQcxIeCuIacqN9mXnvB8LyaO',
                        api_version = 'v2')
    return api

def load_data():
    with open("data/data.pickle", "rb") as f:
        objects = []
        while True:
            try:
                objects.append(pickle.load(f))
            except EOFError:
                break
    return objects[0]

def find_signal(ticker, ticker_data, api, alpha_price, alpha_volume):
    # solid 15 mins
    print(ticker)
    stock_barset = api.get_barset(ticker, '15Min', limit = 27).df.reset_index()
    now_or_last = datetime.now().minute % 15 == 0

    if now_or_last:
        high = (stock_barset.iloc[-1, :].tolist()[2] + stock_barset.iloc[-1, :].tolist()[3]) / 2
        volume = stock_barset.iloc[-1, :].tolist()[-1]
    else:
        high = max((stock_barset.iloc[-2, :].tolist()[2] + stock_barset.iloc[-2, :].tolist()[3]) / 2, (stock_barset.iloc[-1, :].tolist()[2] + stock_barset.iloc[-1, :].tolist()[3]) / 2)
        volume = max(stock_barset.iloc[-2, :].tolist()[-1], stock_barset.iloc[-1, :].tolist()[-1])

    # moving 15 mins
    stock_barset_moving = api.get_barset(ticker, '1Min', limit = 15).df.reset_index()
    idx, last = 0, stock_barset_moving.time[14]
    while (last - stock_barset_moving.time[idx]).seconds > 900:
        idx += 1
    volume_moving = stock_barset_moving.iloc[idx:, -1].sum()
    high_moving = (stock_barset_moving.iloc[idx:, 2].mean() + stock_barset_moving.iloc[idx:, 3].mean()) / 2
    
    current_price = stock_barset_moving.iloc[-1, 2]

    # today high and volume
    today_high_so_far = max(stock_barset.iloc[:, 2])
    today_volume_so_far = max(stock_barset.iloc[:, -1])

    volume_max, high_max = max(max(ticker_data['volume']), today_volume_so_far), max(max(ticker_data['high']), today_high_so_far)
    if high >= alpha_price * high_max and volume >= alpha_volume * volume_max:
        logging.warning(f'Signal - {ticker}, price: {current_price}, volume: {volume} @ {datetime.now()} \nPrevious highest price: {high_max}, volume: {volume_max} \n')
        try:
            share = 10000 // current_price
            response = create_order(ticker, share, 'buy', 'market', 'gtc')
            print(response)
        except:
            pass
    if high_moving >= alpha_price * max(high_max, today_high_so_far) and volume_moving >= alpha_volume * max(volume_max, today_volume_so_far):
        logging.warning(f'Signal (moving) - {ticker}, price: {current_price}, volume: {volume_moving} @ {datetime.now()} \nPrevious highest price: {high_max}, volume: {volume_max} \n')
        try:
            share = 10000 // current_price
            response = create_order(ticker, share, 'buy', 'market', 'gtc')
            print(response)
        except:
            pass
    print(f'{ticker}, volume: {volume_max, volume, volume_moving, today_volume_so_far}, price: {high_max, current_price, high, high_moving, today_high_so_far}')

def run(alpha_price, alpha_volume):
    api = setup()
    data = load_data()
    ticker_list = data.keys()
    logging.warning(f'Start @ {datetime.now()}')

    for ticker in ticker_list:
        try:
            find_signal(ticker, data[ticker], api, alpha_price, alpha_volume)
        except:
            t.sleep(20)
            find_signal(ticker, data[ticker], api, alpha_price, alpha_volume)
            pass

def create_order(symbol, qty, side, order_type, time_in_force):
    data = {
        "symbol": symbol,
        "qty": qty,
        "side": side,
        "type": order_type,
        "time_in_force": time_in_force
    }

    HEADERS = {'APCA-API-KEY-ID': 'PKDLZFEJND51O3Q3TZ3F', 
                'APCA-API-SECRET-KEY': 'xguRqEqr3cw0sNA2cQcxIeCuIacqN9mXnvB8LyaO'}
    BASE_URL = 'https://paper-api.alpaca.markets'
    ORDERS_URL = "{}/v2/orders".format(BASE_URL)

    r = requests.post(ORDERS_URL, json=data, headers=HEADERS)
    return json.loads(r.content)

if __name__ == "__main__":
    # run()
    schedule.every(1).seconds.do(run, alpha_price=0.9, alpha_volume=1.2)
    while True:
        schedule.run_pending()