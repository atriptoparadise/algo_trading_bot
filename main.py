import alpaca_trade_api as tradeapi
import pickle
import time as t
from datetime import datetime, timedelta
import schedule
import numpy as np
import logging
import requests
import json
from config import *

logfile = 'logs/signal_{}.log'.format(datetime.now().date())
logging.basicConfig(filename=logfile, level=logging.WARNING)


class LiveTrade(object):
    def __init__(self, alpha_price, alpha_volume, balance, volatility, high_to_current_ratio, current_to_open_ratio):
        self.alpha_price = alpha_price
        self.alpha_volume = alpha_volume
        self.balance = balance
        self.limit_order = balance / (volatility * 3)
        self.api = None
        self.current_to_open_ratio = current_to_open_ratio
        self.high_to_current_ratio = high_to_current_ratio
        self.holding_stocks = []

    def setup(self):
        self.api = tradeapi.REST(API_KEY, 
                                SECRET_KEY, 
                                api_version = 'v2')
        data = self.load_data('data')
        return data, data.keys()

    def load_data(self, filename):
        with open(f"data/{filename}.pickle", "rb") as f:
            objects = []
            while True:
                try:
                    objects.append(pickle.load(f))
                except EOFError:
                    break
        return objects[0]

    def get_positions(self):
        response = requests.get("{}/v2/positions".format(BASE_URL), headers=HEADERS)
        content = json.loads(response.content)
        self.holding_stocks = [item['symbol'] for item in content]
        return content

    def get_today_max(self, ticker):
        stock_barset = self.api.get_barset(ticker, '15Min', limit = 27).df.reset_index()
        return max(stock_barset.iloc[:-1, 2]), max(stock_barset.iloc[:-1, -1])

    def high_volume_moving_15m(self, ticker):
        stock_barset_moving = self.api.get_barset(ticker, '1Min', limit = 15).df.reset_index()
        idx, last = 0, stock_barset_moving.time[14]
        while (last - stock_barset_moving.time[idx]).seconds > 900:
            idx += 1
        volume_moving = stock_barset_moving.iloc[idx:, -1].sum()
        current_price = stock_barset_moving.iloc[-1, -2]

        return volume_moving, current_price

    def high_current_check(self, ticker, current_price):
        if datetime.now().weekday() >= 5 or datetime.now().hour < 9 or (datetime.now().hour == 9 and datetime.now().minute < 30):
            logging.warning(f'Signal pre-market - {ticker}, price: {current_price} @ {datetime.now()}')
            return False, 0, 0
        stock_barset = self.api.get_barset(ticker, '1Min', limit = 390).df.reset_index()
        idx = 0
        while stock_barset.time[idx].date() < datetime.now().date() or stock_barset.time[idx].hour < 9:
            idx += 1
        while stock_barset.time[idx].minute < 30:
            idx += 1
        
        high = stock_barset.iloc[idx:, 2].max()
        open_price = stock_barset.iloc[idx, 1]

        if datetime.now().hour < 15:
            return True, open_price, 1
        if datetime.now().hour >= 16:
            logging.warning(f'Signal after-hours - {ticker}, price: {current_price} @ {datetime.now()}')
            logging.warning(f'High close check: {current_price > open_price and (high - current_price) / (current_price - open_price) <= self.high_to_current_ratio}')
            return False, 0, 0
        if current_price > open_price and (high - current_price) / (current_price - open_price) <= self.high_to_current_ratio:
            return True, open_price, 3
        logging.warning(f"Signal can't satisfy high current check - {ticker}, price: {current_price} @ {datetime.now()}")
        return False, open_price, 0

    def if_exceed_high(self, current_price, high_list, high_max):
        if current_price < high_max:
            return 1, False
        idx_high = np.argmax(np.array(high_list))
        if len(high_list) - idx_high >= 20:
            return 1.5, True
        return 1, False

    def find_signal(self, ticker, ticker_data):
        volume_moving, current_price = self.high_volume_moving_15m(ticker)
        volume_max, high_max = max(ticker_data['volume']), max(ticker_data['high'])

        if current_price >= self.alpha_price * high_max and volume_moving >= self.alpha_volume * volume_max:
            today_high_so_far, today_volume_so_far = self.get_today_max(ticker)
            try:
                good, open_price, after_3pm = self.high_current_check(ticker, current_price)
            except:
                return
            if current_price < self.alpha_price * today_high_so_far or volume_moving < self.alpha_volume * today_volume_so_far:
                logging.warning(f'Find first signal but today high/volume cannot fit - {ticker}, price: {current_price}, volume moving: {volume_moving} @ {datetime.now()} \nPrevious highest price: {high_max, today_high_so_far}, volume: {volume_max, today_volume_so_far} \n')
                return
            if not good or current_price >= self.current_to_open_ratio * open_price:
                logging.warning(f'Find first signal but not good - {ticker}, price: {current_price}, volume moving: {volume_moving} @ {datetime.now()} \nPrevious highest price: {high_max, today_high_so_far}, volume: {volume_max, today_volume_so_far} \n')
                return
            
            exceed_high, exceeded = self.if_exceed_high(current_price, ticker_data['high'], high_max)
            try:
                response = self.create_order(symbol=ticker, 
                                            qty=self.limit_order * after_3pm * exceed_high // current_price, 
                                            side='buy', 
                                            order_type='market', 
                                            time_in_force='day')
            except:
                logging.warning('Order failed')
                pass
            if exceeded:
                logging.warning(f'{ticker} exceeded at lease 20 days high')
            logging.warning(f'Signal - {ticker}, price: {current_price}, volume moving: {volume_moving} @ {datetime.now()} \nPrevious highest price: {high_max, today_high_so_far}, volume: {volume_max, today_volume_so_far} \n')
            print(f'{ticker}, volume: {volume_max} - {volume_moving}, price: {high_max} - {current_price}')

    def run(self):
        data, ticker_list = self.setup()
        positions = self.get_positions()
        run_list = [ticker for ticker in ticker_list if ticker not in self.holding_stocks]
        logging.warning(f'Start @ {datetime.now()}')

        for ticker in run_list:
            try:
                self.find_signal(ticker, data[ticker])
            except:
                t.sleep(20)
                self.find_signal(ticker, data[ticker])
                pass

    def create_order(self, symbol, qty, side, order_type, time_in_force):
        data = {
            "symbol": symbol,
            "qty": qty,
            "side": side,
            "type": order_type,
            "time_in_force": time_in_force
        }

        r = requests.post(ORDERS_URL, json=data, headers=HEADERS)
        return json.loads(r.content)


if __name__ == "__main__":
    trade = LiveTrade(alpha_price=0.9, alpha_volume=1.3, balance=8000, 
                        volatility=8, high_to_current_ratio=0.2, current_to_open_ratio=1.15)
    schedule.every(1).seconds.do(trade.run)
    while True:
        schedule.run_pending()