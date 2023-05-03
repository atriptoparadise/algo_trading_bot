import pickle
import pandas as pd
from datetime import datetime, timedelta
from joblib import Parallel, delayed
from pandas.tseries.offsets import BDay
import schedule
import numpy as np
import logging
import requests
import json
from config import *

logfile = 'logs/signal_{}.log'.format(datetime.now().date())
logging.basicConfig(filename=logfile, level=logging.WARNING)


class LiveTrade(object):
    def __init__(self, alpha_price, alpha_volume, order_amount, high_to_current_ratio, current_to_open_ratio):
        self.alpha_price = alpha_price
        self.alpha_volume = alpha_volume
        self.order_amount = order_amount
        self.current_to_open_ratio = current_to_open_ratio
        self.high_to_current_ratio = high_to_current_ratio
        self.holding_stocks = []

    def setup(self):
        self.get_holding_stocks()
        data = self.load_data('data')
        ticker_list = data.keys()
        run_list = [
            ticker for ticker in ticker_list if ticker not in self.holding_stocks]
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
        response = requests.get(
            "{}/v2/positions".format(API_URL), headers=HEADERS)
        content = json.loads(response.content)
        self.holding_stocks = [item['symbol'] for item in content]

    def get_bid_ask_spread_ratio(self, ticker):
        response = requests.get(
            f'{POLY_URL}/v3/quotes/{ticker}/?sort=asc&apiKey={POLY_KEY}')
        res = json.loads(response.content)['results']

        ask = res[0]['ask_price']
        bid = res[0]['bid_price']
        return round(100 * (ask - bid) / ((ask + bid) / 2), 3)

    def get_moving_volume(self, ticker, date):
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

    def nine_days_close_check(self, ticker, current_price, today):
        """Return True if current price is higher than close price nine business days ago"""

        nine_days = (datetime.strptime(today, '%Y-%m-%d') - BDay(9)).date()
        response = requests.get(
            f'{POLY_URL}/v1/open-close/{ticker}/{nine_days}?apiKey={POLY_KEY}')

        try:
            nine_days_close = json.loads(response.content)['close']
            if current_price >= nine_days_close:
                return True, nine_days_close
            return False, nine_days_close
        except:
            return False, 'NaN'

    def high_current_check(self, current_price, open_price, high):
        if current_price > open_price and (high - current_price) / (current_price - open_price) <= self.high_to_current_ratio:
            return True, 2
        return False, 0

    def get_open_price(self, ticker, date):
        """Return today's open and high"""
        if datetime.now().hour == 9 and datetime.now().minute < 30:
            return 100000, 100000

        response = requests.get(
            f'{POLY_URL}/v2/aggs/ticker/{ticker}/range/1/day/{date}/{date}?sort=desc&apiKey={POLY_KEY}')
        content = json.loads(response.content)['results']
        return content[0]['o'], content[0]['h']

    def if_exceed_high(self, current_price, high_list, time_list, prev_high):
        if current_price < prev_high:
            return False
        idx_high = np.argmax(np.array(high_list))
        high_time = time_list[idx_high]
        days_delta = (datetime.now() -
                      datetime.strptime(high_time, '%Y-%m-%d %H:%M:%S')).days
        if days_delta >= 20:
            return True
        return False

    def add_data(self, ticker, today, order, after_3pm, good, exceed_nine_days_close, exceeded, volume_moving, prev_vol_max, current_price, prev_high, open_price, bid_ask_spread):
        date = datetime.strptime(today, '%Y-%m-%d').date()
        time = datetime.now().strftime("%H:%M:%S")
        weekday = int(date.weekday()) + 1

        if datetime.now().hour < 15:
            high_current_check = 'None'
        elif good:
            high_current_check = 1
        else:
            high_current_check = 0

        new_signal = [ticker, date, time, ticker + date.strftime('%Y/%m/%d') + str(order), order,
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

    def get_high_so_far(self, ticker, date):
        response = requests.get(
            f'{POLY_URL}/v2/aggs/ticker/{ticker}/range/1/minute/{date}/{date}?sort=asc&apiKey={POLY_KEY}')
        content = json.loads(response.content)['results']
        start = datetime.now().replace(hour=13, minute=30)

        for idx, item in enumerate(content):
            time = datetime.utcfromtimestamp(item['t'] / 1000)
            if time >= start - timedelta(minutes=1):
                break

        return max([item['h'] for item in content[idx:]])

    def open_high_check(self, ticker, open_price, current_price, date):
        high = self.get_high_so_far(ticker, date)

        if current_price <= self.current_to_open_ratio * open_price and high <= open_price * 1.25:
            return True
        return False

    def is_signal_one(self, current_price, prev_high):
        """Signal 1:
            vol >= vol_max * 85%; 
            price >= prev_high; 
            curr > 1; 
            before 10 am
        """
        if current_price >= self.alpha_price * prev_high and datetime.now().hour == 9:
            return True
        return False

    def is_signal_two(self, volume_moving, prev_vol_max, current_price, open_price):
        """Signal 2:
            vol >= vol_max; 
            curr > open * 1.15; 
            curr > 1; 
            before 12 pm
        """
        if volume_moving >= prev_vol_max and current_price > open_price * self.current_to_open_ratio and datetime.now().hour < 12:
            return True
        return False

    def find_signal(self, ticker, ticker_data, today):
        logfile = 'logs/signal_{}.log'.format(datetime.now().date())
        logging.basicConfig(filename=logfile, level=logging.WARNING)

        try:
            volume_moving, current_price = self.get_moving_volume(
                ticker, today)
            prev_vol_max, prev_high = max(
                ticker_data['volume']), max(ticker_data['high'])

            # Signal 1 - vol >= vol_max * 85%; price >= prev_high; curr > 1; before 10 am
            # Signal 2 - vol >= vol_max; curr > open * 1.15; curr > 1; before 12 pm

            # minimal conditions: vol >= vol_max * 85% + curr > 1
            if volume_moving >= self.alpha_volume * prev_vol_max and current_price > 1:
                open_price, high = self.get_open_price(ticker, today)

                if self.is_signal_one(current_price, prev_high) or self.is_signal_two(volume_moving, prev_vol_max, current_price, open_price):
                    qty = self.order_amount // current_price

                    self.create_order(symbol=ticker, qty=qty, side='buy',
                                      order_type='market', time_in_force='day')
                    self.trailing_stop_order(
                        symbol=ticker, qty=qty, trail_percent=1)

                    print(
                        f'{ticker} - ordered!, price: {current_price}, volume moving: {volume_moving} @ {datetime.now()}')
                    logging.warning(
                        f'{ticker} - ordered!, price: {current_price}, volume moving: {volume_moving} @ {datetime.now()}')
                    logging.warning('-' * 60)
                    logging.warning('')
                    order = 1

                if datetime.now().hour < 15:
                    exceed_nine_days_close, nine_days_close = True, 0
                    good, after_3pm = True, 1
                else:
                    exceed_nine_days_close, nine_days_close = self.nine_days_close_check(
                        ticker, current_price, today)
                    good, after_3pm = self.high_current_check(
                        current_price, open_price, high)

                exceeded = self.if_exceed_high(
                    current_price, ticker_data['high'], ticker_data['time'], prev_high)

                bid_ask_spread = self.get_bid_ask_spread_ratio(ticker)
                self.add_data(ticker, today, order, after_3pm, good, exceed_nine_days_close, exceeded,
                              volume_moving, prev_vol_max, current_price, prev_high, open_price, bid_ask_spread)

                if not good:
                    logging.warning('')
                    return

                if current_price < open_price:
                    print(
                        f'{ticker} current price ({current_price}) is lower than open price ({open_price})')
                    logging.warning(
                        f'{ticker} current price ({current_price}) is lower than open price ({open_price}) \n')
                    self.add_data(ticker, today, 0, after_3pm, good, exceed_nine_days_close, exceeded,
                                  volume_moving, prev_vol_max, current_price, prev_high, open_price, bid_ask_spread)
                    return

                if not self.open_high_check(ticker, open_price):
                    print(
                        f'{ticker} current price ({current_price}) is higher than {self.current_to_open_ratio} * open price ({open_price})')
                    logging.warning(
                        f'{ticker} current price ({current_price}) is higher than {self.current_to_open_ratio} * open price ({open_price}) \n')
                    self.add_data(ticker, today, 0, after_3pm, good, exceed_nine_days_close, exceeded,
                                  volume_moving, prev_vol_max, current_price, prev_high, open_price, bid_ask_spread)
                    return

                if not exceed_nine_days_close:
                    print(
                        f'{ticker} cannot exceed nine days close - current price: {current_price}, nine days close: {nine_days_close}')
                    logging.warning(
                        f'{ticker} cannot exceed nine days close - current price: {current_price}, nine days close: {nine_days_close} \n')
                    self.add_data(ticker, today, 0, after_3pm, good, exceed_nine_days_close, exceeded,
                                  volume_moving, prev_vol_max, current_price, prev_high, open_price, bid_ask_spread)
                    return

                if datetime.now().hour < 15 and not exceeded:
                    print(
                        f'{ticker} before 15:00 but cannot exceed previous 20 days high - current price: {current_price}, previous high: {prev_high}')
                    logging.warning(
                        f'{ticker} before 15:00 but cannot exceed previous 20 days high - current price: {current_price}, previous high: {prev_high} \n')
                    self.add_data(ticker, today, 0, after_3pm, good, exceed_nine_days_close, exceeded,
                                  volume_moving, prev_vol_max, current_price, prev_high, open_price, bid_ask_spread)
                    return

                if current_price <= 1:
                    print(
                        (f'{ticker} - penny stock, current price: {current_price}'))
                    logging.warning(
                        f'{ticker} - penny stock, current price: {current_price} \n')
                    self.add_data(ticker, today, 0, after_3pm, good, exceed_nine_days_close, exceeded,
                                  volume_moving, prev_vol_max, current_price, prev_high, open_price, bid_ask_spread)
                    return
        except:
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
        logging.warning((json.loads(r.content)))
        return json.loads(r.content)

    def trailing_stop_order(self, symbol, qty, trail_percent):
        data = {
            "side": "sell",
            "symbol": symbol,
            "type": "trailing_stop",
            "qty": qty,
            "time_in_force": "day",
            "trail_percent": trail_percent
        }

        r = requests.post(ORDERS_URL, json=data, headers=HEADERS)
        logging.warning((json.loads(r.content)))
        return json.loads(r.content)

    def run(self, date=None):
        data, run_list = self.setup()
        if not date:
            date = datetime.today().strftime('%Y-%m-%d')

        print(f'\nStart @ {datetime.now()}')
        Parallel(n_jobs=4)(delayed(self.find_signal)(
            ticker, data[ticker], date) for ticker in run_list)


if __name__ == "__main__":
    trade = LiveTrade(alpha_price=1, alpha_volume=0.85, order_amount=ORDER_AMOUNT,
                      high_to_current_ratio=0.2, current_to_open_ratio=1.15)
    schedule.every(1).seconds.do(trade.run)
    while True:
        schedule.run_pending()
