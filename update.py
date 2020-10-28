from collections import deque
import numpy as np
from iexfinance.stocks import get_historical_intraday
import alpaca_trade_api as tradeapi
from datetime import datetime, timedelta
import time as t
import datetime as dt
import pickle
from config import *


class UpdateData(object):
    def __init__(self, token, data_file, ticker_file):
        self._data = {}
        self.token = token
        self.file_name = data_file
        self.ticker_file = ticker_file
        self.api = tradeapi.REST(API_KEY, SECRET_KEY, api_version = 'v2')

    def save_data(self):
        with open(self.file_name, 'wb') as f:
            pickle.dump(self._data, f, pickle.HIGHEST_PROTOCOL)

    def load_data(self):
        with open(self.file_name, "rb") as f:
            objects = []
            while True:
                try:
                    objects.append(pickle.load(f))
                except EOFError:
                    break
        self._data = objects[0]

    def get_price_close_open(self, data_list):
        start, end = 0, -1
        try:
            while not ('close' in data_list[end] and data_list[end]['close']):
                end -= 1
            while not ('open' in data_list[start] and data_list[start]['open']):
                start += 1

            diff = data_list[end]['close'] - data_list[start]['open']
            if data_list[start]['open'] == 0:
                return diff, None
            return diff, 100 * (diff / data_list[start]['open'])
        except:
            return None, None
    
    def get_volume_close_open(self, data_list):
        start, end = 0, -1
        try:
            while not ('volume' in data_list[end] and data_list[end]['volume']):
                end -= 1
            while not ('volume' in data_list[start] and data_list[start]['volume']):
                start += 1

            diff = data_list[end]['volume'] - data_list[start]['volume']
            if data_list[start]['volume'] == 0:
                return diff, None
            return diff, 100 * (diff / data_list[start]['volume'])
        except:
            return None, None

    def get_high_volume_day(self, ticker, date):
        data = self.api.get_barset(ticker, '15Min', limit = 1000).df.reset_index()
        data['date'] = [i.date() for i in data.time]
        return max(data[data.date == date].iloc[:, 2]), max(data[data.date == date].iloc[:, -2])

    def get_volume_list(self, ticker):
        data = self.api.get_barset(ticker, '15Min', limit = 1000).df.reset_index()
        data['date'] = [i.date() for i in data.time]
        time = data.date.unique()
        volume_max = []
        for i in time:
            if self.midnight == False:
                volume_max.append(data[data.date == i].iloc[:, -2].max())
            else:
                if i != datetime.now().date():
                    volume_max.append(data[data.date == i].iloc[:, -2].max())
        return volume_max
    
    def get_high_list(self, ticker):
        data = self.api.get_barset(ticker, '15Min', limit = 1000).df.reset_index()
        data['date'] = [i.date() for i in data.time]
        time = data.date.unique()
        high_max = []
        for i in time:
            if self.midnight == False:
                high_max.append(data[data.date == i].iloc[:, 2].max())
            else:
                if i != datetime.now().date():
                    high_max.append(data[data.date == i].iloc[:, 2].max())
        return high_max

    def init_data(self, ticker):
        print(f'{ticker} initializes ..')

        start_date = datetime.now().date()
        if self.midnight:
            start_date = start_date - timedelta(1)
        while start_date.weekday() >= 5:
            start_date = start_date - timedelta(1)

        today_data_list = get_historical_intraday(ticker, start_date, token=self.token)
        price_close_open, price_ratio = self.get_price_close_open(today_data_list)
        volume_close_open, volume_ratio = self.get_volume_close_open(today_data_list)
        high_30_days = self.get_high_list(ticker)
        volume_30_days = self.get_volume_list(ticker)

        if not self._data:
            self._data = {ticker: {'volume': volume_30_days,
                                    'high': high_30_days,
                                    'date': start_date.date(),
                                    'price_close_open': price_close_open,
                                    'volume_close_open': volume_close_open,
                                    'price_ratio': price_ratio,
                                    'volume_ratio': volume_ratio
                                    }}
        else:
            self._data.update({ticker: {'volume': volume_30_days,
                                    'high': high_30_days,
                                    'date': start_date,
                                    'price_close_open': price_close_open,
                                    'volume_close_open': volume_close_open,
                                    'price_ratio': price_ratio,
                                    'volume_ratio': volume_ratio
                                    }})
        
    def update_data(self, ticker):
        print(f'{ticker} updates ..')
        start_date = datetime.now().date()
        if self.midnight:
            start_date = start_date - timedelta(1)

        updated_date = self._data[ticker]['date']

        if updated_date == start_date:
            print(f'{ticker} already up-to-date')
            return
        
        days_delta = (start_date - updated_date).days
        for i in range(days_delta):
            date = updated_date + timedelta(i + 1)
            if date.weekday() >= 5:
                continue

            data_list = get_historical_intraday(ticker, date, token=self.token)
            high_today, volume_today = self.get_high_volume_day(ticker, date)

            self._data[ticker]['high'].append(high_today)
            self._data[ticker]['volume'].append(volume_today)
            self._data[ticker]['date'] = date
            self._data[ticker]['price_close_open'], self._data[ticker]['price_ratio'] = self.get_price_close_open(data_list)
            self._data[ticker]['volume_close_open'], self._data[ticker]['volume_ratio'] = self.get_volume_close_open(data_list)

    def clean_data(self):
        if not self._data:
            return

        remove_list = []
        for ticker in self._data.keys():
            if sum([i > 0 for i in self._data[ticker]['volume']]) < 5 or sum([i > 0 for i in self._data[ticker]['high']]) < 5:
            # if not all(i > 0 for i in self._data[ticker]['volume']) or not all(i > 0 for i in self._data[ticker]['high']):
                remove_list.append(ticker)
                print(f'removed {ticker}')

        if not remove_list:
            return
        for ticker in remove_list:
            self._data.pop(ticker)

    def update(self, ticker):
        if ticker not in self._data.keys():
            self.init_data(ticker)
        else:
            self.update_data(ticker)

    def load_watch_list(self):
        with open(self.ticker_file, "rb") as f:
            objects = []
            while True:
                try:
                    objects.append(pickle.load(f))
                except EOFError:
                    break
        return objects[0]

    def run(self, watch_list=None, midnight=False):
        print('')
        print('load data ..')
        self.load_data()
        print('')
        self.midnight = midnight

        if not watch_list:
            ticker_list = self.load_watch_list()
            for ticker in ticker_list:
                try:
                    self.update(ticker)
                    print('Done')
                    print('')  
                except:
                    print(f'{ticker} fails to update')
                    print('')
                    pass
        else:
            for ticker in watch_list:
                try:
                    self.update(ticker)
                    print('Done')
                    print('')  
                except:
                    print(f'{ticker} fails to update')
                    print('')
                    pass

        if self._data:
            self.clean_data()
            self.save_data()
            print('cleaned')
        print(f'data saved - {len(self._data)} stocks')


if __name__ == "__main__":
    update = UpdateData(TOKEN, 'data/data.pickle', 'data/watch_list.pickle')
    update.run(watch_list=None, midnight=True)