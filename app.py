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
        run_list = [
            ticker for ticker in FG_list if ticker not in self.holding_stocks and ticker not in SKIP_LIST]
        return run_list

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

    def is_signal_fairy_guide(self, ticker, date):
        o, h, l, c = utils.get_last_5min_ohlc(ticker, date)
        is_fairy_guide, upper_lead_ratio, bottom_lead_ratio, body_ratio = utils.is_fairy_guide(o, h, l, c)
        if is_fairy_guide:
            return True, upper_lead_ratio, bottom_lead_ratio, body_ratio
        return False, upper_lead_ratio, bottom_lead_ratio, body_ratio

    def find_signal(self, ticker, today):
        logfile = 'logs/signal_{}.log'.format(datetime.now().date())
        logging.basicConfig(filename=logfile, level=logging.WARNING)

        try:
            bid_ask_spread = utils.get_bid_ask_spread_ratio(ticker)
            current_price = utils.get_last_close(ticker, today)
            is_fairy_guide, upper_lead_ratio, bottom_lead_ratio, body_ratio = self.is_signal_fairy_guide(ticker, today)
            if is_fairy_guide and current_price > 1 and bid_ask_spread < 0.3 and bid_ask_spread >= 0:
                # Round up
                qty = self.order_amount // current_price + 1

                self.create_order(symbol=ticker, qty=qty, side='buy',
                                    order_type='market', time_in_force='ioc')
                
                utils.log_print_text_fg(
                            ticker, current_price, upper_lead_ratio, 
                            bottom_lead_ratio, body_ratio, bid_ask_spread,
                            send_text=True, signal_type='Fairy Guide')
                
                self.trailing_stop_order(
                    symbol=ticker, buy_qty=qty, trail_percent=2)
                logging.warning(
                    f'{ticker} - Trailing stop order created @ {datetime.now()}/n' + '-' * 60 + '/n')

        except Exception as e:
            print(e, ticker)
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
        run_list = self.setup()
        if not date:
            date = datetime.today().strftime('%Y-%m-%d')

        print(f'\nStart @ {datetime.now()}')
        Parallel(n_jobs=-1)(delayed(self.find_signal)(
            ticker, date) for ticker in run_list)


if __name__ == "__main__":
    trade = LiveTrade(breakout_ratio=1, vol_ratio=0.85, order_amount=ORDER_AMOUNT,
                      high_to_current_ratio=0.2, current_to_open_ratio=1.15)
    schedule.every(10).seconds.do(trade.run)
    while True:
        schedule.run_pending()
