import alpaca_trade_api as tradeapi
from datetime import datetime, timedelta
import pytz
import schedule
import logging
import requests
import json
import time as t
from config import *

logfile = 'logs/signal_{}.log'.format(datetime.now().date())
logging.basicConfig(filename=logfile, level=logging.WARNING)
eastern = pytz.timezone('US/Eastern')


class PortfolioMonitor(object):
    def __init__(self):
        self.closed_orders = []
        self.api = tradeapi.REST(API_KEY, 
                                SECRET_KEY, 
                                api_version = 'v2')

    def get_positions(self):
        response = requests.get("{}/v2/positions".format(API_URL), headers=HEADERS)
        content = json.loads(response.content)
        self.holding_stocks = [item['symbol'] for item in content]
        return content

    def get_closed_orders(self):
        response = requests.get(ORDERED_URL, headers=HEADERS)
        content = json.loads(response.content)
        self.closed_orders = [item for item in content if item['status'] == 'filled' or item['status'] == 'closed']

    def get_highest_price(self, ticker):
        order_details = next(item for item in self.closed_orders if item['symbol'] == ticker)
        ordered_time = eastern.localize(datetime.strptime(order_details['filled_at'], '%Y-%m-%dT%H:%M:%S.%fZ')) - timedelta(hours=4)
        stock_barset = self.api.get_barset(ticker, '1Min', limit = 390).df.reset_index()
        
        idx = 0
        while idx < 390 and stock_barset.time[idx] < ordered_time - timedelta(minutes=1):
            idx += 1
        return stock_barset.iloc[idx:, 2].max()

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

    def portfolio_monitor(self, ticker, positions, stop_ratio, stop_earning_ratio, stop_earning_ratio_high):
        data = next(item for item in positions if item['symbol'] == ticker)
        current_price, entry_price, qty = float(data['current_price']), float(data['avg_entry_price']), float(data['qty'])

        if current_price <= stop_ratio * entry_price:
            try:
                self.create_order(symbol=ticker, qty=qty, side='sell', order_type='market', time_in_force='gtc')
                logging.warning(f'Sold {qty} shares of {ticker} at {current_price} v.s. {entry_price} @ {datetime.now()}')
                print((f'Sold {qty} shares of {ticker} at {current_price} v.s. {entry_price} @ {datetime.now()}'))
            except:
                logging.warning(f'Failed to sell {ticker} at {current_price} v.s. {entry_price} @ {datetime.now()}')
                print(f'Failed to sell {ticker} at {current_price} v.s. {entry_price} @ {datetime.now()}')
                pass
        
        if current_price >= 1.1 * entry_price and entry_price * qty >= 500:
            try:
                self.create_order(symbol=ticker, qty=qty // 3, side='sell', order_type='market', time_in_force='gtc')
                logging.warning(f'Sold {qty} shares of {ticker} at {current_price} v.s. {entry_price} @ {datetime.now()}')
                print((f'Sold {qty} shares of {ticker} at {current_price} v.s. {entry_price} @ {datetime.now()}'))
            except:
                logging.warning(f'Failed to sell {ticker} at {current_price} v.s. {entry_price} @ {datetime.now()}')
                print(f'Failed to sell {ticker} at {current_price} v.s. {entry_price} @ {datetime.now()}')
                pass

        highest_price = self.get_highest_price(ticker)
        earning_ratio = highest_price / entry_price
        if highest_price >= stop_earning_ratio_high * entry_price \
            and current_price <= entry_price * (earning_ratio - 0.1):            
            try:
                self.create_order(symbol=ticker, qty=qty, side='sell', order_type='market', time_in_force='gtc')
                logging.warning(f'Sold {qty} shares of {ticker} at {current_price} v.s. entry price {entry_price} v.s. highest price {highest_price} @ {datetime.now()}')
                print(f'Sold {qty} shares of {ticker} at {current_price} v.s. entry price {entry_price} v.s. highest price {highest_price} @ {datetime.now()}')
            except:
                logging.warning(f'Failed to sell {ticker} at {current_price} v.s. {entry_price} v.s. highest price {highest_price} @ {datetime.now()}')
                print(f'Failed to sell {ticker} at {current_price} v.s. {entry_price} @ {datetime.now()}')
                pass
    
    def run(self, stop_ratio, stop_earning_ratio, stop_earning_ratio_high):
        positions = self.get_positions()
        self.get_closed_orders()
        monitoring_list = [ticker for ticker in self.holding_stocks if ticker not in IGNORE_LIST]

        if not monitoring_list:
            return

        for ticker in monitoring_list:
            try:
                self.portfolio_monitor(ticker, positions, stop_ratio, stop_earning_ratio, stop_earning_ratio_high)
            except:
                self.portfolio_monitor(ticker, positions, stop_ratio, stop_earning_ratio, stop_earning_ratio_high)
                pass


if __name__ == "__main__":
    monitor = PortfolioMonitor()
    schedule.every(30).seconds.do(monitor.run, stop_ratio=0.9, stop_earning_ratio=0.5, stop_earning_ratio_high=1.1)
    while True:
        schedule.run_pending()
