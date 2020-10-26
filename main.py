import alpaca_trade_api as tradeapi
import pickle
import time as t
from datetime import datetime, timedelta
import schedule
import logging
import requests
import json
from config import *

logfile = 'logs/signal_{}.log'.format(datetime.now().date())
logging.basicConfig(filename=logfile, level=logging.WARNING)


class LiveTrade(object):
    def __init__(self, alpha_price, alpha_volume, balance, volatility, stop_ratio, high_to_current_ratio, current_to_open_ratio):
        self.alpha_price = alpha_price
        self.alpha_volume = alpha_volume
        self.balance = balance
        self.limit_order = balance / volatility
        self.api = None
        self.stop_ratio = stop_ratio
        self.current_to_open_ratio = current_to_open_ratio
        self.high_to_current_ratio = high_to_current_ratio
        self.holding_stocks = {}

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
        
    def save_data(self, filename):
        with open(f"data/{filename}.pickle", 'wb') as f:
            pickle.dump(self.holding_stocks, f, pickle.HIGHEST_PROTOCOL)

    def high_volume_fix_15m(self, ticker):
        stock_barset = self.api.get_barset(ticker, '15Min', limit = 27).df.reset_index()
        now_or_last = datetime.now().minute % 15 == 0

        if now_or_last:
            high = (stock_barset.iloc[-1, :].tolist()[2] + stock_barset.iloc[-1, :].tolist()[3]) / 2
            volume = stock_barset.iloc[-1, :].tolist()[-1]
        else:
            high = max((stock_barset.iloc[-2, :].tolist()[2] + stock_barset.iloc[-2, :].tolist()[3]) / 2, (stock_barset.iloc[-1, :].tolist()[2] + stock_barset.iloc[-1, :].tolist()[3]) / 2)
            volume = max(stock_barset.iloc[-2, :].tolist()[-1], stock_barset.iloc[-1, :].tolist()[-1])
        
        today_high_so_far = max(stock_barset.iloc[:-1, 2])
        today_volume_so_far = max(stock_barset.iloc[:-1, -1])
        return high, volume, today_high_so_far, today_volume_so_far

    def high_volume_moving_15m(self, ticker):
        stock_barset_moving = self.api.get_barset(ticker, '1Min', limit = 15).df.reset_index()
        idx, last = 0, stock_barset_moving.time[14]
        while (last - stock_barset_moving.time[idx]).seconds > 900:
            idx += 1
        volume_moving = stock_barset_moving.iloc[idx:, -1].sum()
        high_moving = (stock_barset_moving.iloc[idx:, 2].mean() + stock_barset_moving.iloc[idx:, 3].mean()) / 2

        current_price = stock_barset_moving.iloc[-1, 2]
        return high_moving, volume_moving, current_price

    def holding_risk_control(self, ticker):
        current_price = self.api.get_barset(ticker, '1Min', limit = 1)[ticker][-1].c
        if current_price <= self.stop_ratio * self.holding_stocks[ticker][0]:
            self.create_order(symbol=ticker, 
                            qty=self.holding_stocks[ticker][1], 
                            side='sell',
                            order_type='market',
                            time_in_force='gtc')
            self.holding_stocks.pop(ticker)

    def high_current_check(self, ticker, current_price):
        if datetime.now().weekday() >= 5 or datetime.now().hour >= 16 or datetime.now().hour < 9:
            logging.warning(f'Signal after hours - {ticker}, price: {current_price} @ {datetime.now()}')
            return False, 0
        stock_barset = self.api.get_barset(ticker, '1Min', limit = 390).df.reset_index()
        idx = 0
        while stock_barset.time[idx].day < datetime.now().day or stock_barset.time[idx].hour < 9:
            idx += 1
        while stock_barset.time[idx].minute < 30:
            idx += 1
        
        high = stock_barset.iloc[idx:, 2].max()
        open_price = stock_barset.iloc[idx, 1]

        if datetime.now().hour < 15 or (current_price > open_price and (high - current_price) / (current_price - open_price) <= self.high_to_current_ratio):
            return True, open_price
        return False, open_price

    def find_signal(self, ticker, ticker_data):
        if ticker in self.holding_stocks:
            self.holding_risk_control(ticker)
            return

        print(ticker)
        high_fix, volume_fix, today_high_so_far, today_volume_so_far = self.high_volume_fix_15m(ticker)
        high_moving, volume_moving, current_price = self.high_volume_moving_15m(ticker)
        volume_max, high_max = max(max(ticker_data['volume']), today_volume_so_far), max(max(ticker_data['high']), today_high_so_far)
        
        if current_price >= self.alpha_price * high_max and max(volume_fix, volume_moving) >= self.alpha_volume * volume_max:
            good, open_price = self.high_current_check(ticker, current_price)
            if not good or current_price >= self.current_to_open_ratio * open_price:
                return
            try:
                response = self.create_order(symbol=ticker, 
                                            qty=self.limit_order // current_price, 
                                            side='buy', 
                                            order_type='market', 
                                            time_in_force='day')
                self.holding_stocks.update({ticker: (current_price, self.limit_order // current_price)})
                # logging.warning(response)
            except:
                logging.warning('Order failed')
                pass
            logging.warning(f'Signal - {ticker}, price: {current_price}, volume fix: {volume_fix}, volume moving: {volume_moving} @ {datetime.now()} \nPrevious highest price: {high_max}, volume: {volume_max} \n')
        
        print(f'{ticker}, volume: {volume_max, volume_fix, volume_moving, today_volume_so_far}, price: {high_max, current_price, high_fix, high_moving, today_high_so_far}')

    def run(self):
        data, ticker_list = self.setup()
        self.holding_stocks = self.load_data('holding')
        logging.warning(f'Start @ {datetime.now()}')

        for ticker in ticker_list:
            try:
                self.find_signal(ticker, data[ticker])
            except:
                t.sleep(20)
                self.find_signal(ticker, data[ticker])
                pass
        
        self.save_data('holding')

    def create_order(self, symbol, qty, side, order_type, time_in_force):
        data = {
            "symbol": symbol,
            "qty": qty,
            "side": side,
            "type": order_type,
            "time_in_force": time_in_force
        }

        HEADERS = {'APCA-API-KEY-ID': API_KEY, 'APCA-API-SECRET-KEY': SECRET_KEY}
        ORDERS_URL = "{}/v2/orders".format(BASE_URL)

        r = requests.post(ORDERS_URL, json=data, headers=HEADERS)
        return json.loads(r.content)


if __name__ == "__main__":
    trade = LiveTrade(alpha_price=0.9, alpha_volume=1.3, balance=100000, 
                        volatility=10, stop_ratio=0.95, high_to_current_ratio=0.2,
                        current_to_open_ratio=1.15)
    schedule.every(1).seconds.do(trade.run)
    while True:
        schedule.run_pending()