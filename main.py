import alpaca_trade_api as tradeapi
import pickle
import time as t
from datetime import datetime, timedelta
from joblib import Parallel, delayed
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
        self.get_holding_stocks()
        data = self.load_data('data_new')
        ticker_list = data.keys()
        run_list = [ticker for ticker in ticker_list if ticker not in self.holding_stocks]
        return data, run_list

    def load_data(self, filename):
        with open(f"data/{filename}.pickle", "rb") as f:
            objects = []
            while True:
                try:
                    objects.append(pickle.load(f))
                except EOFError:
                    break
        return objects[0]

    def get_holding_stocks(self):
        response = requests.get("{}/v2/positions".format(BASE_URL), headers=HEADERS)
        content = json.loads(response.content)
        self.holding_stocks = [item['symbol'] for item in content]

    def get_moving_volume(self, ticker, date):
        """Return 15-min moving aggregated volume and last price"""

        response = requests.get(f'{POLY_URL}/v2/aggs/ticker/{ticker}/range/1/minute/{date}/{date}?sort=desc&apiKey={API_KEY}')
        content = json.loads(response.content)['results']
        last_time = content[0]['t']
        idx = 14
        if len(content) < 15:
            idx = -1
        while last_time - content[idx]['t'] > 900000:
            idx -= 1
        return sum([i['v'] for i in content[:idx + 1]]), content[0]['c']

    def nine_days_close_check(self, ticker, current_price, today):
        today = datetime.strptime(today, '%Y-%m-%d')
        nine_days = '2020-10-20'
        response = requests.get(f'{POLY_URL}/v1/open-close/{ticker}/{nine_days}?apiKey={API_KEY}')
        nine_days_close = json.loads(response.content)['close']
        if current_price >= nine_days_close:
            return True, nine_days_close
        return False, nine_days_close

    def high_current_check(self, ticker, current_price, volume_moving):
        if datetime.now().weekday() >= 5 or datetime.now().hour < 9 or (datetime.now().hour == 9 and datetime.now().minute < 30):
            logging.warning(f'Signal weekend or before 9:30 - {ticker}, price: {current_price}, moving volume: {volume_moving} @ {datetime.now()}')
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
            logging.warning(f'Signal before 15:00 - {ticker}, price: {current_price}, moving volume: {volume_moving} @ {datetime.now()}')
            return True, open_price, 1
        if datetime.now().hour >= 16:
            logging.warning(f'Signal after 16:00 - {ticker}, price: {current_price}, moving volume: {volume_moving} @ {datetime.now()}')
            logging.warning(f'High close check: {current_price > open_price and (high - current_price) / (current_price - open_price) <= self.high_to_current_ratio}')
            return False, 0, 0
        if current_price > open_price and (high - current_price) / (current_price - open_price) <= self.high_to_current_ratio:
            logging.warning(f'Signal after 15:00 and high current check good - {ticker}, price: {current_price}, moving volume: {volume_moving} @ {datetime.now()}')
            return True, open_price, 3
        logging.warning(f"Signal can't satisfy high current check - {ticker}, price: {current_price}, moving volume: {volume_moving} @ {datetime.now()}")
        return False, open_price, 0

    def if_exceed_high(self, current_price, high_list, time_list, high_max):
        if current_price < high_max:
            return 1, False
        idx_high = np.argmax(np.array(high_list))
        high_time = time_list[idx_high]
        days_delta = (datetime.now() - datetime.strptime(high_time, '%Y-%m-%d %H:%M:%S')).days
        if days_delta >= 20:
            return 1.5, True
        return 1, False

    def find_signal(self, ticker, ticker_data, today):
        logfile = 'logs/signal_{}.log'.format(datetime.now().date())
        logging.basicConfig(filename=logfile, level=logging.WARNING)

        try:
            volume_moving, current_price = self.get_moving_volume(ticker, today)
            volume_max, high_max = max(ticker_data['volume']), max(ticker_data['high'])

            if current_price >= self.alpha_price * high_max and volume_moving >= self.alpha_volume * volume_max:
                good, open_price, after_3pm = self.high_current_check(ticker, current_price, volume_moving)
                if not good:
                    logging.warning(f'Previous highest price: {high_max}, volume: {volume_max} \n')
                    return

                if current_price >= self.current_to_open_ratio * open_price:
                    logging.warning(f'Current price ({current_price}) is higher than {self.current_to_open_ratio} * open price ({open_price})')
                    logging.warning(f'Previous highest price: {high_max}, volume: {volume_max} \n')
                    return
                
                exceed_nine_days_close, nine_days_close = self.nine_days_close_check(ticker, current_price, today)
                if not exceed_nine_days_close:
                    logging.warning(f'Cannot exceed nine days close - current price: {current_price}, nine_days_close: {nine_days_close}')
                    logging.warning(f'Previous highest price: {high_max}, volume: {volume_max} \n')
                    return
                
                exceed_high, exceeded = self.if_exceed_high(current_price, ticker_data['high'], ticker_data['time'], high_max)
                response = self.create_order(symbol=ticker, 
                                            qty=self.limit_order * after_3pm * exceed_high // current_price, 
                                            side='buy', 
                                            order_type='market', 
                                            time_in_force='day')
                if exceeded:
                    logging.warning(f'{ticker} exceeded at least 20 days high')
                
                logging.warning(f'Signal - {ticker}, price: {current_price}, volume moving: {volume_moving} @ {datetime.now()} - Previous highest price: {high_max}, volume: {volume_max} \n')
                print(f'{ticker}, volume: {volume_max} - {volume_moving}, price: {high_max} - {current_price}')
        except IndexError:
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

    def run(self, date=None):
        data, run_list = self.setup()
        if not date:
            date = datetime.today().strftime('%Y-%m-%d')

        print(f'Start @ {datetime.now()}')
        Parallel(n_jobs=4)(delayed(self.find_signal)(ticker, data[ticker], date) for ticker in run_list)


if __name__ == "__main__":
    trade = LiveTrade(alpha_price=0.9, alpha_volume=1.3, balance=8000, 
                        volatility=8, high_to_current_ratio=0.2, current_to_open_ratio=1.15)
    schedule.every(1).seconds.do(trade.run)
    while True:
        schedule.run_pending()
