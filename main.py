import alpaca_trade_api as tradeapi
import pickle
import time as t
from datetime import datetime
import schedule
import logging
import requests
import json

logfile = 'logs/signal_{}.log'.format(datetime.now().date())
logging.basicConfig(filename=logfile, level=logging.WARNING)


class LiveTrade(object):
    def __init__(self, alpha_price, alpha_volume, balance, volatility):
        self.alpha_price = alpha_price
        self.alpha_volume = alpha_volume
        self.balance = balance
        self.limit_order = balance / volatility
        self.holding_stocks = []
        self.api = None

    def setup(self):
        self.api = tradeapi.REST('PKDLZFEJND51O3Q3TZ3F', 
                            'xguRqEqr3cw0sNA2cQcxIeCuIacqN9mXnvB8LyaO',
                            api_version = 'v2')

    def load_data(self):
        with open("data/data.pickle", "rb") as f:
            objects = []
            while True:
                try:
                    objects.append(pickle.load(f))
                except EOFError:
                    break
        return objects[0]

    def find_signal(self, ticker, ticker_data):
        # solid 15 mins
        print(ticker)
        stock_barset = self.api.get_barset(ticker, '15Min', limit = 27).df.reset_index()
        now_or_last = datetime.now().minute % 15 == 0

        if now_or_last:
            high = (stock_barset.iloc[-1, :].tolist()[2] + stock_barset.iloc[-1, :].tolist()[3]) / 2
            volume = stock_barset.iloc[-1, :].tolist()[-1]
        else:
            high = max((stock_barset.iloc[-2, :].tolist()[2] + stock_barset.iloc[-2, :].tolist()[3]) / 2, (stock_barset.iloc[-1, :].tolist()[2] + stock_barset.iloc[-1, :].tolist()[3]) / 2)
            volume = max(stock_barset.iloc[-2, :].tolist()[-1], stock_barset.iloc[-1, :].tolist()[-1])

        # moving 15 mins
        stock_barset_moving = self.api.get_barset(ticker, '1Min', limit = 15).df.reset_index()
        idx, last = 0, stock_barset_moving.time[14]
        while (last - stock_barset_moving.time[idx]).seconds > 900:
            idx += 1
        volume_moving = stock_barset_moving.iloc[idx:, -1].sum()
        high_moving = (stock_barset_moving.iloc[idx:, 2].mean() + stock_barset_moving.iloc[idx:, 3].mean()) / 2

        
        current_price = stock_barset_moving.iloc[-1, 2]
        today_high_so_far = max(stock_barset.iloc[:, 2])
        today_volume_so_far = max(stock_barset.iloc[:, -1])

        volume_max, high_max = max(max(ticker_data['volume']), today_volume_so_far), max(max(ticker_data['high']), today_high_so_far)
        if high >= self.alpha_price * high_max and max(volume, volume_moving) >= self.alpha_volume * volume_max and ticker not in self.holding_stocks:
            try:
                response = self.create_order(ticker, self.limit_order // current_price, 'buy', 'market', 'gtc')
                self.holding_stocks.append(ticker)
                logging.warning(response)
            except:
                logging.warning('Order failed')
                pass
            logging.warning(f'Signal - {ticker}, price: {current_price}, volume: {volume}, volume moving: {volume_moving} @ {datetime.now()} \nPrevious highest price: {high_max}, volume: {volume_max} \n')
        
        print(f'{ticker}, volume: {volume_max, volume, volume_moving, today_volume_so_far}, price: {high_max, current_price, high, high_moving, today_high_so_far}')

    def run(self):
        self.setup()
        data = self.load_data()
        ticker_list = data.keys()

        logging.warning(f'Start @ {datetime.now()}')

        for ticker in ticker_list:
            try:
                self.find_signal(ticker, data[ticker])
            except:
                t.sleep(20)
                self.find_signal(ticker, data[ticker])
                pass

    def create_order(self, symbol, qty, side, order_type, time_in_force):
        data = {
            "symbol": symbol,
            "qty": qty,
            "side": side,
            "type": order_type,
            "time_in_force": time_in_force
        }

        HEADERS = {'APCA-API-KEY-ID': 'PKDLZFEJND51O3Q3TZ3F', 
                    'APCA-API-SECRET-KEY': 'xguRqEqr3cw0sNA2cQcxIeCuIacqN9mXnvB8LyaO'}
        BASE_URL = 'https://paper-api.alpaca.markets'
        ORDERS_URL = "{}/v2/orders".format(BASE_URL)

        r = requests.post(ORDERS_URL, json=data, headers=HEADERS)
        return json.loads(r.content)

if __name__ == "__main__":
    trade = LiveTrade(alpha_price=0.9, alpha_volume=1.2, balance=100000, volatility=10)
    schedule.every(1).seconds.do(trade.run)
    while True:
        schedule.run_pending()