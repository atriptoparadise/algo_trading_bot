from main import LiveTrade
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


class PortfolioMonitor(LiveTrade):
    def __init__(self, alpha_price, alpha_volume, balance, volatility, stop_ratio, high_to_current_ratio, current_to_open_ratio, stop_earning_ratio, stop_earning_ratio_high):
        super().__init__(alpha_price, alpha_volume, balance, volatility, stop_ratio, high_to_current_ratio, current_to_open_ratio, stop_earning_ratio, stop_earning_ratio_high)
        self.closed_orders = []
        self.api = tradeapi.REST(API_KEY, 
                                SECRET_KEY, 
                                api_version = 'v2')

    def run(self):
        positions = self.get_positions()
        self.get_closed_orders()
        logging.warning(f'Portfolio monitor start @ {datetime.now()}')

        for ticker in self.holding_stocks:
            try:
                self.portfolio_monitor(ticker, positions)
            except:
                t.sleep(20)
                self.portfolio_monitor(ticker, positions)
                pass

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

    def portfolio_monitor(self, ticker, positions):
        data = next(item for item in positions if item['symbol'] == ticker)
        current_price, entry_price, qty = float(data['current_price']), float(data['avg_entry_price']), float(data['qty'])

        if current_price <= self.stop_ratio * entry_price:
            try:
                self.create_order(symbol=ticker, qty=qty, side='sell', order_type='market', time_in_force='gtc')
                logging.warning(f'Sold {ticker} at {current_price} v.s. {entry_price} @ {datetime.now()}')
                print((f'Sold {ticker} at {current_price} v.s. {entry_price} @ {datetime.now()}'))
            except:
                logging.warning(f'Failed to sell {ticker} at {current_price} v.s. {entry_price} @ {datetime.now()}')
                pass
        
        highest_price = self.get_highest_price(ticker)
        if highest_price > self.stop_earning_ratio_high * entry_price and (current_price - entry_price) / (highest_price - entry_price) <= self.stop_earning_ratio:
            try:
                self.create_order(symbol=ticker, qty=qty, side='sell', order_type='market', time_in_force='gtc')
                logging.warning(f'Sold {ticker} at {current_price} v.s. entry price {entry_price} v.s. highest price {highest_price} @ {datetime.now()}')
                print(f'Sold {ticker} at {current_price} v.s. entry price {entry_price} v.s. highest price {highest_price} @ {datetime.now()}')
            except:
                logging.warning(f'Failed to sell {ticker} at {current_price} v.s. {entry_price} v.s. highest price {highest_price} @ {datetime.now()}')
                pass


if __name__ == "__main__":
    monitor = PortfolioMonitor(alpha_price=0.9, alpha_volume=1.3, balance=100000, 
                        volatility=10, stop_ratio=0.96, high_to_current_ratio=0.2,
                        current_to_open_ratio=1.15, stop_earning_ratio=0.5, stop_earning_ratio_high=1.07)
    schedule.every(59).seconds.do(monitor.run)
    while True:
        schedule.run_pending()
