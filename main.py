from datetime import datetime
from joblib import Parallel, delayed
import schedule
import logging
import requests
import json
import time as t
from config import *
import utils

logfile = 'logs/signal_{}.log'.format(datetime.now().date())
logging.basicConfig(filename=logfile, level=logging.WARNING)


class LiveTrade(object):
    def __init__(self, breakout_ratio, vol_ratio, order_amount, high_to_current_ratio, current_to_open_ratio):
        self.breakout_ratio = breakout_ratio
        self.vol_ratio = vol_ratio
        self.order_amount = order_amount
        self.current_to_open_ratio = current_to_open_ratio
        self.high_to_current_ratio = high_to_current_ratio
        self.holding_stocks = []
        self.open_time = datetime.today().replace(
            hour=9, minute=30, second=0, microsecond=0)

    def setup(self):
        self.get_holding_stocks()
        data = utils.load_data('data')
        ticker_list = data.keys()
        run_list = [
            ticker for ticker in ticker_list if ticker not in self.holding_stocks and ticker not in SKIP_LIST]
        return data, run_list

    def get_holding_stocks(self):
        response = requests.get(
            "{}/v2/positions".format(API_URL), headers=HEADERS)
        content = json.loads(response.content)
        self.holding_stocks = [item['symbol'] for item in content]

    def get_holding_qty(self, ticker):
        response = requests.get(
                "{}/v2/positions".format(API_URL), headers=HEADERS)
        content = json.loads(response.content)
        for item in content:
            if item['symbol'] == ticker:
                return float(item['qty'])
        return 0

    def is_signal_one(self, current_price, prev_high, day_high):
        """Signal 1:
            vol >= prev_vol_max * 85%; 
            price >= prev_high & day_high; 
            curr > 1; 
            before 10 am
        """
        if current_price >= self.breakout_ratio * max(prev_high, day_high * 0.9) and datetime.now().hour == 9:
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
            bid_ask_spread = utils.get_bid_ask_spread_ratio(ticker)

            # Signal 1 - vol >= vol_max * 85%; price >= prev_high & day_high; curr > 1; spread_ratio <= 0.2%; before 10 am
            # Signal 2 - vol >= vol_max; curr > open * 1.15; curr > 1; spread_ratio <= 0.2%; before 12 pm
            # minimal conditions: vol >= vol_max * 85%; curr > 1; spread_ratio <= 0.2%
            if volume_moving >= self.vol_ratio * prev_vol_max and current_price > 1 and bid_ask_spread < 0.3 and bid_ask_spread >= 0:
                open_price, day_high = utils.get_open_price(ticker, today)

                # Signal 1 & 2
                if self.is_signal_one(current_price, prev_high, day_high) or self.is_signal_two(volume_moving, prev_vol_max, current_price, open_price):
                    # remove below if-else when real trading: pre hours wont execute mkt order
                    # Trading hours
                    if datetime.now() >= self.open_time:
                        # Round up
                        qty = self.order_amount // current_price + 1

                        self.create_order(symbol=ticker, qty=qty, side='buy',
                                          order_type='market', time_in_force='ioc')

                        # Add Signal type
                        if self.is_signal_one(current_price, prev_high, day_high) and self.is_signal_two(volume_moving, prev_vol_max, current_price, open_price):
                            signal_type = 'Signal 1 & 2!'
                        elif self.is_signal_one(current_price, prev_high, day_high):
                            signal_type = 'Signal 1!'
                        else:
                            signal_type = 'Signal 2!'

                        utils.log_print_text(
                            ticker, current_price, prev_high, day_high, volume_moving, 
                            prev_vol_max, bid_ask_spread, ticker_data['beta'], ticker_data['mkt_cap_string'], 
                            send_text=True, signal_type=signal_type)
                        
                        # t.sleep(1)
                        self.trailing_stop_order(
                            symbol=ticker, buy_qty=qty, trail_percent=2)
                        logging.warning(
                            f'{ticker} - Trailing stop order created @ {datetime.now()}/n' + '-' * 60 + '/n')
                    
                    # Pre hours
                    else:
                        utils.log_print_text(
                            ticker, current_price, prev_high, day_high, volume_moving, 
                            prev_vol_max, bid_ask_spread, ticker_data['beta'], ticker_data['mkt_cap_string'], 
                            send_text=True, signal_type='Pre-hours')
                
                # Only satisfies minimal conditions
                else:
                    utils.log_print_text(
                        ticker, current_price, prev_high, day_high, volume_moving, 
                        prev_vol_max, bid_ask_spread, ticker_data['beta'], ticker_data['mkt_cap_string'],
                        send_text=False, signal_type='min. condition')
                    
                # Add to csv for all satisfy minimal conditions
                utils.check_other_condi_add_signal(ticker, current_price, today, open_price,
                                                   day_high, self.high_to_current_ratio, ticker_data,
                                                   prev_high, signal_type, volume_moving, prev_vol_max, bid_ask_spread)

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

    def trailing_stop_order(self, symbol, buy_qty, trail_percent):
        qty = self.get_holding_qty(symbol)
        data = {
            "side": "sell",
            "symbol": symbol,
            "type": "trailing_stop",
            "qty": qty,
            "time_in_force": "day",
            "trail_percent": trail_percent
        }

        if qty != buy_qty:
            print(f'{symbol} filed trailing stop on {buy_qty - qty} over buy_qty {buy_qty}')
            logging.warning(f'{symbol} filed trailing stop on {buy_qty - qty} over buy_qty {buy_qty}')

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
    trade = LiveTrade(breakout_ratio=1, vol_ratio=0.85, order_amount=ORDER_AMOUNT,
                      high_to_current_ratio=0.2, current_to_open_ratio=1.15)
    schedule.every(1).seconds.do(trade.run)
    while True:
        schedule.run_pending()
