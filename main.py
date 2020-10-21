import os
import alpaca_trade_api as tradeapi
import pandas as pd 
import pickle
import time as t
from datetime import datetime
import schedule
from logger import logger_setup, FuncTimer
import logging
import logging.config

logfile = 'logs/signal_{}.log'.format(datetime.now().date())
logging.basicConfig(filename=logfile, level=logging.WARNING)

def setup():
    os.environ['APCA_API_BASE_URL'] = "https://paper-api.alpaca.markets"
    api = tradeapi.REST('PKURD8LXNN3ET9MLQ3Q0', 
                        'x0YwJnP9HzCAUBO4DAYogSUKEPrj3FckNniYdetg',
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

def find_signal(ticker, ticker_data, api):
    # solid 15 mins
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
    volume_moving = stock_barset_moving.iloc[:, -1].sum()
    high_moving = (stock_barset_moving.iloc[:, 2].mean() + stock_barset_moving.iloc[:, 3].mean()) / 2
    
    # today high and volume
    today_high_so_far = max(stock_barset.iloc[:, 2])
    today_volume_so_far = max(stock_barset.iloc[:, -1])

    volume_max, high_max = max(ticker_data['volume']), max(ticker_data['high'])
    if high >= 0.9 * max(high_max, today_high_so_far) and volume >= 1.3 * max(volume_max, today_volume_so_far):
        logging.warning(f'Find Signal - {ticker}, price: {high}, volume: {volume} @ {datetime.now()}')
        logging.warning(f'Previous high: {high_max}, volume: {volume_max}')
    if high_moving >= 0.9 * max(high_max, today_high_so_far) and volume_moving >= 1.5 * max(volume_max, today_volume_so_far):
        logging.warning(f'Find Signal moving - {ticker}, price: {high_moving}, volume: {volume_moving} @ {datetime.now()}')
        logging.warning(f'Previous high: {high_max}, volume: {volume_max}')

def run():
    api = setup()
    data = load_data()
    ticker_list = data.keys()
    logging.warning(f'Start @ {datetime.now()}')
    for ticker in ticker_list:
        try:
            find_signal(ticker, data[ticker], api)
        except:
            t.sleep(20)
            find_signal(ticker, data[ticker], api)
            pass

if __name__ == "__main__":
    # run()
    schedule.every(1).seconds.do(run)
    while True:
        schedule.run_pending()