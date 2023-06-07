from datetime import datetime
from joblib import Parallel, delayed
import schedule
import logging
import requests
import json
import pandas as pd
from config import *
import utils

logfile = 'logs/signal_{}.log'.format(datetime.now().date())
logging.basicConfig(filename=logfile, level=logging.WARNING)


class LiveTrade(object):
    def __init__(self, order_amount, curr_to_open_ratio):
        self.order_amount = order_amount
        self.curr_to_open_ratio = curr_to_open_ratio
        self.holding_stocks = []
        self.open_time_fg = datetime.today().replace(
            hour=9, minute=45, second=0, microsecond=0)
        self.open_time = datetime.today().replace(
            hour=9, minute=30, second=0, microsecond=0)

    def setup(self):
        self.get_holding_stocks()
        run_list = [
            ticker for ticker in FG_LIST if ticker not in self.holding_stocks and ticker not in SKIP_LIST]
        
        signal_df = pd.read_csv('data/signals.csv', index_col=0)
        signal_list = signal_df.symbol_date.unique()
        signal_list_date = datetime.today().strftime('%Y/%m/%d')
        return run_list, signal_list, signal_list_date

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
        o, h, l, c, is_up_trend = utils.get_last_5min_ohlc(ticker, date)
        is_fairy_guide, upper_lead_ratio, bottom_lead_ratio, body_ratio = utils.is_fairy_guide(
            o, h, l, c, upper_lead_ratio=5, bottom_lead_ratio=1, body_ratio=0.0001)

        return is_fairy_guide, is_up_trend, upper_lead_ratio, bottom_lead_ratio, body_ratio

    def is_signal_momentum(self, ticker, curr, open, signal_list, signal_list_date):
        ticker_date = ticker + signal_list_date + "momentum"
        if curr >= open * 1.01 and ticker_date not in signal_list:
            return True
        return False
    
    def find_signal(self, ticker, today, signal_list, signal_list_date):
        logfile = 'logs/signal_{}.log'.format(datetime.now().date())
        logging.basicConfig(filename=logfile, level=logging.WARNING)

        try:
            current_price = utils.get_last_close(ticker, today)
            if ticker in MOM_LIST and datetime.now() >= self.open_time and datetime.now().hour < 10:
                open_price, _ = utils.get_open_price(ticker, today)
                if self.is_signal_momentum(ticker, current_price, open_price, signal_list, signal_list_date):
                    # Round up
                    qty = (self.order_amount / 2) // current_price + 1

                    self.create_order(symbol=ticker, qty=qty, side='buy',
                                      order_type='market', time_in_force='ioc')
                    
                    utils.log_print_text_mom(ticker, current_price, open_price, 
                                             send_text=True, signal_type='momentum')
                    
                    self.trailing_stop_order(
                        symbol=ticker, buy_qty=qty, trail_percent=1)
                    logging.warning(
                        f'{ticker} - Trailing stop order created @ {datetime.now()}/n' + '-' * 60 + '/n')
                
            bid_ask_spread = utils.get_bid_ask_spread_ratio(ticker)
            is_fairy_guide, is_up_trend, upper_lead_ratio, bottom_lead_ratio, body_ratio = self.is_signal_fairy_guide(
                ticker, today)
            if is_fairy_guide and current_price > 1 and bid_ask_spread < 0.3 and bid_ask_spread >= 0:
                open_price, _ = utils.get_open_price(ticker, today)
                curr_to_open = 100 * current_price / open_price - 100
                
                # Check if uptrend, curr_to_open not too high, and time 
                if is_up_trend and curr_to_open < self.curr_to_open_ratio and datetime.now() >= self.open_time_fg and datetime.now().hour < 12:
                    # Round up
                    qty = self.order_amount // current_price + 1

                    self.create_order(symbol=ticker, qty=qty, side='buy',
                                      order_type='market', time_in_force='ioc')

                    utils.log_print_text_fg(
                        ticker, current_price, curr_to_open, is_up_trend, 
                        upper_lead_ratio, bottom_lead_ratio, body_ratio, bid_ask_spread,
                        send_text=True, signal_type='Fairy Guide!')

                    self.trailing_stop_order(
                        symbol=ticker, buy_qty=qty, trail_percent=2)
                    logging.warning(
                        f'{ticker} - Trailing stop order created @ {datetime.now()}/n' + '-' * 60 + '/n')
                else:
                    utils.log_print_text_fg(
                        ticker, current_price, curr_to_open, is_up_trend, 
                        upper_lead_ratio, bottom_lead_ratio, body_ratio, bid_ask_spread,
                        send_text=False, signal_type='Fairy not in good time or uptrend or curr too high')

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
            print(
                f'{symbol} filed trailing stop on {buy_qty - qty} over buy_qty {buy_qty}')
            logging.warning(
                f'{symbol} filed trailing stop on {buy_qty - qty} over buy_qty {buy_qty}')

        r = requests.post(ORDERS_URL, json=data, headers=HEADERS)
        logging.warning((json.loads(r.content)))
        return json.loads(r.content)

    def run(self, date=None):
        run_list, signal_list, signal_list_date = self.setup()
        if not date:
            date = datetime.today().strftime('%Y-%m-%d')

        print(f'\nStart @ {datetime.now()}')
        Parallel(n_jobs=-1)(delayed(self.find_signal)(
            ticker, date, signal_list, signal_list_date) for ticker in run_list)


if __name__ == "__main__":
    trade = LiveTrade(order_amount=FG_ORDER_AMOUNT,
                      curr_to_open_ratio=15)
    schedule.every(10).seconds.do(trade.run)
    while True:
        schedule.run_pending()
