from BackTest.back_test import BackTest
from collections import deque
import numpy as np
from iexfinance.stocks import get_historical_intraday
from datetime import datetime, timedelta
import pickle


class BackTest15Min(BackTest):
    def __init__(self, token, period, balance, volatility):
        super().__init__(token, period, balance, volatility)
        self._data = None

    def get_volume_list(self, data_list):
        slot = len(data_list) // 15
        slot_plus = len(data_list) % 15
        slot_today = []
        for i in range(slot):
            slot_each = 0
            for j in range(15):
                idx = 15 * i + j
                if 'volume' in data_list[idx] and data_list[idx]['volume']:
                    slot_each += data_list[idx]['volume']
            slot_today.append(slot_each)

        if slot_plus > 0:
            slot_each = 0
            for j in range(slot_plus):
                idx = slot * 15 + j
                if 'volume' in data_list[idx] and data_list[idx]['volume']:
                    slot_each += data_list[idx]['volume']
            slot_today.append(slot_each)
        return slot_today

    def get_high_day(self, data_list):
        high_today = -1
        for i in range(len(data_list)):
            if 'high' in data_list[i] and data_list[i]['high']:
                high_today = max(high_today, data_list[i]['high'])
        return high_today

    def set_up_15_min(self, ticker):
        high_29_days = -1
        volume_max_29_days = -1

        for i in reversed(range(30)):
            if i == 0:
                break

            date = (datetime.now() - timedelta(i)).date()
            if date.weekday() >= 5:
                continue
            
            data_list = get_historical_intraday(ticker, date, token=self.token)

            high_29_today = max(high_29_days, self.get_high_day(data_list))
            list_volume_today = self.get_volume_list(data_list)
            volume_max_today = np.max(list_volume_today)
            volume_max_29_days = max(volume_max_29_days, volume_max_today)

        return high_29_days, volume_max_29_days

    def get_high_list(self, data_list):
        slot = len(data_list) // 15
        slot_plus = len(data_list) % 15
        slot_today = []
        for i in range(slot):
            slot_each = -1
            for j in range(15):
                idx = 15 * i + j
                if 'high' in data_list[idx] and data_list[idx]['high']:
                    slot_each = max(data_list[idx]['high'], slot_each)
            slot_today.append(slot_each)

        if slot_plus > 0:
            slot_each = -1
            for j in range(slot_plus):
                idx = slot * 15 + j
                if 'high' in data_list[idx] and data_list[idx]['high']:
                    slot_each = max(data_list[idx]['high'], slot_each)
            slot_today.append(slot_each)
        return slot_today

    def set_up_today(self, ticker):
        date = datetime.now().date()
        data_list = get_historical_intraday(ticker, date, token=self.token)

        list_volume_current = self.get_volume_list(data_list)
        list_high_current = self.get_high_list(data_list)

        if len(list_high_current) != len(list_volume_current):
            return
        
        combo_high_volume = []
        for i in range(len(list_high_current)):
            combo_high_volume.append((list_high_current[i], list_volume_current[i]))
        
        return combo_high_volume

    def find_signal(self, ticker, alpha_price, alpha_volume):
        print('*' * 45)
        print(f'Symbol: {ticker}')
        combo_high_volume = self.set_up_today(ticker)
        if not combo_high_volume:
            print('Skip this symbol')
            return

        previous_high, previous_volume = self.set_up_15_min(ticker)
        for i in range(len(combo_high_volume)):
            if combo_high_volume[i][0] >= previous_high * alpha_price and combo_high_volume[i][1] >= previous_volume * alpha_volume:
                print(f'Find Signal at {i + 1} 15 mins slot')
        print('No Signal')
        print('')

    def run(self, ticker_list, alpha_price, alpha_volume):
        print('')
        print('Start new test...')
        print('')
        for ticker in ticker_list:
            self.find_signal(ticker, alpha_price, alpha_volume)

    def update_data(self, ticker, midnight = False):
        if midnight:
            today_date = datetime.now().date() - timedelta(1)
        else:
            today_date = datetime.now().date()
    
    def upload_data(self, ticker):
        if self._data:
            return
        with open("data.pickle", 'rb') as f:
            objects = []
            while True:
                try:
                    objects.append(pickle.load(f))
                except EOFError:
                    break
        self._data = objects[0]

    def init_data(self, ticker):
        today_date = datetime.now().date() - timedelta(1)


if __name__ == "__main__":
    ticker_list = ['WDFC', 'GSX', 'CRSP', 'ERIC', 'CHU', 'FVRR', 'PHR', 'CMBM', 'ESTA', 'DAO']
    token = 'sk_17b078529ba34b7396ef93de2d19b287'
    test = BackTest15Min(token=token, period=5, balance=8000, volatility=8)
    test.run(ticker_list, alpha_price=0.9, alpha_volume=1.3)


    
