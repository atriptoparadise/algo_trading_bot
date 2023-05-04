from datetime import datetime
from joblib import Parallel, delayed
import schedule
import logging
import requests
import json
from config import *
import utils

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
        self.open_time = datetime.today().replace(hour=9, minute=30, second=0, microsecond=0)

    def setup(self):
        self.get_holding_stocks()
        data = utils.load_data('data')
        ticker_list = data.keys()
        run_list = [
            ticker for ticker in ticker_list if ticker not in self.holding_stocks]
        return data, run_list


    def get_holding_stocks(self):
        response = requests.get(
            "{}/v2/positions".format(API_URL), headers=HEADERS)
        content = json.loads(response.content)
        self.holding_stocks = [item['symbol'] for item in content]


    def is_signal_one(self, current_price, prev_high):
        """Signal 1:
            vol >= prev_vol_max * 85%; 
            price >= prev_high; 
            curr > 1; 
            before 10 am
        """
        if current_price >= self.alpha_price * prev_high and datetime.now().hour == 9:
            return True
        return False

    def is_signal_two(self, volume_moving, prev_vol_max, current_price, open_price):
        """Signal 2:
            vol >= prev_vol_max; 
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
            volume_moving, current_price = utils.get_moving_volume(
                ticker, today)
            prev_vol_max, prev_high = max(
                ticker_data['volume']), max(ticker_data['high'])

            # Signal 1 - vol >= vol_max * 85%; price >= prev_high; curr > 1; before 10 am
            # Signal 2 - vol >= vol_max; curr > open * 1.15; curr > 1; before 12 pm
            # minimal conditions: vol >= vol_max * 85% + curr > 1
            if volume_moving >= self.alpha_volume * prev_vol_max and current_price > 1:
                open_price, day_high = utils.get_open_price(ticker, today)
                bid_ask_spread = utils.get_bid_ask_spread_ratio(ticker)

                if self.is_signal_one(current_price, prev_high) or self.is_signal_two(volume_moving, prev_vol_max, current_price, open_price):
                    # remove below if-else when real trading: pre hours wont execute mkt order
                    if datetime.now() >= self.open_time:
                        qty = self.order_amount // current_price

                        self.create_order(symbol=ticker, qty=qty, side='buy',
                                        order_type='market', time_in_force='day')
                        
                        utils.log_print_text(ticker, current_price, volume_moving, bid_ask_spread, send_text=True, is_order=1)

                        self.trailing_stop_order(
                            symbol=ticker, qty=qty, trail_percent=1)
                        logging.warning(
                            f'{ticker} - Trailing stop order created @ {datetime.now()}/n' + '-' * 60 + '/n')
                        order = 1
                    else:
                        utils.log_print_text(ticker, current_price, volume_moving, bid_ask_spread, send_text=True, is_order=0)
                        order = 0
                else:
                    utils.log_print_text(ticker, current_price, volume_moving, bid_ask_spread, send_text=False, is_order=2)
                    order = 0

                utils.check_other_condi_add_signal(ticker, current_price, today, open_price, 
                                                   day_high, self.high_to_current_ratio, ticker_data, 
                                                   prev_high, order, volume_moving, prev_vol_max, bid_ask_spread)

        except Exception as e:
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
        Parallel(n_jobs=-1)(delayed(self.find_signal)(
            ticker, data[ticker], date) for ticker in run_list)


if __name__ == "__main__":
    trade = LiveTrade(alpha_price=1, alpha_volume=0.85, order_amount=ORDER_AMOUNT,
                      high_to_current_ratio=0.2, current_to_open_ratio=1.15)
    schedule.every(1).seconds.do(trade.run)
    while True:
        schedule.run_pending()
