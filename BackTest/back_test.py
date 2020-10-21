from datetime import datetime, timedelta
from iexfinance.stocks import get_historical_intraday
from collections import deque
import numpy as np


class BackTest(object):
    def __init__(self, token, period, balance, volatility):
        self.high = -1
        self.token = token
        self.period = period
        self.balance_all = balance
        self.volatility = volatility
        self.balance_one = balance / volatility
        self.max_volume_period = deque([0 for i in range(period)])
        self.max_high_period = deque([0 for i in range(period)])
        self._sell_day = None
        self._sell_minute = None

    def get_last_price(self, traded_price, ticker):
        date = datetime.now().date()
        x = 1
        while not get_historical_intraday(ticker, date, token = self.token):
            date = (datetime.now() - timedelta(x)).date()
            x += 1
        last_price = get_historical_intraday(symbol=ticker, date=date, token = self.token)[-1]['close']
        if last_price:
            print(f'newest price: {last_price}, newest price ratio: {round(100 * (last_price / traded_price - 1), 2)}')

    def condition_volume(self, current_volume, max_volume_period, alpha_volume):
        if current_volume and current_volume > alpha_volume * max(max_volume_period) and any(max_volume_period):
            return True
        return False

    def condition_high(self, current_price, max_high_period, alpha_price):
        if current_price and any(max_high_period) and current_price > alpha_price * max(max_high_period):
            return True
        return False

    def check_price_volume(self, data_list, threshold_price, threshold_volume):
        idx = 0
        try:
            while not 'close' in data_list[idx] or not data_list[idx]['close'] or not 'volume' in data_list[idx] or not data_list[idx]['volume']:
                idx += 1
            if data_list[idx]['close'] <= threshold_price or data_list[idx]['volume'] <= threshold_volume:
                print(f"Skip this stock - Volume: {data_list[idx]['volume']}, Price: {data_list[idx]['close']}")
                return False
            return True
        except:
            return True

    def check_close_and_open_last_day(self, date, ticker):
        date = date - timedelta(1)
        while not get_historical_intraday(ticker, date, token = self.token):
            date = date - timedelta(1)
            if (datetime.now().date() - date).days > 30:
                return True

        idx_open, idx_close = 0, -1
        data_list = get_historical_intraday(ticker, date, token = self.token)
        try:
            while not ('open' in data_list[idx_open] and 'close' in data_list[idx_close] and data_list[idx_open]['open'] and data_list[idx_close]['close']):
                idx_close -= 1
                idx_open += 1
            if data_list[idx_close]['close'] >= data_list[idx_open]['open']:
                return True 
            return False
        except:
            return True

    def condition_close_high_average(self, idx, data_list, high_today, avg_today):
        if idx >= len(data_list) * 0.92 and avg_today < 0.98 * high_today:
            print('-- High variance today')
            return False
        return True

    def find_signal(self, ticker, sell_ratio=0.95, alpha_volume=1, alpha_price=1, sell_to_keep=0.7):
        print('*' * 45)
        print(f'Symbol: {ticker}')
        balance = self.balance_one
        amount = balance
        for i in reversed(range(30)):
            date = (datetime.now() - timedelta(i)).date()
            if date.weekday() >= 5:
                continue
            max_volume_today = -1
            high_today = -1
            updated_today = False
            data_list = get_historical_intraday(ticker, date, token = self.token)
            today_list = []
            hold = False
            avg_today = []
            # if not self.check_price_volume(data_list, 1, 1):
            #     return
            
            if not self.check_close_and_open_last_day(date, ticker):
                # Skip today
                continue

            for i in range(len(data_list)):
                if 'high' in data_list and data_list[i]['high']:
                    high_today = max(high_today, data_list[i]['high'])
                    today_list.append(data_list[i]['high'])

                if 'close' in data_list[i] and data_list[i]['close']:
                    avg_today.append(data_list[i]['close'])

                if (today_list and np.mean(today_list) <= 0.975 * high_today):
                    print(f'!! Notice - high variance today {date}')

                if 'volume' in data_list[i] and self.condition_volume(data_list[i]['volume'], self.max_volume_period, alpha_volume) and 'close' in data_list[i] and self.condition_high(data_list[i]['close'], self.max_high_period, alpha_price) and all(i > 0 for i in self.max_volume_period):
                    #  and (avg_today and self.condition_close_high_average(i, data_list, high_today, np.mean(avg_today)) or not avg_today):
                    if self._sell_day and date < self._sell_day:
                        print(f"-- Find new signal at {data_list[i]['close']} - {date} {data_list[i]['minute']}")
                        break
                    if self._sell_day and self._sell_minute and date == self._sell_day and data_list[i]['minute'] <= self._sell_minute:
                        print(f"-- Find new signal at {data_list[i]['close']} - {date} {data_list[i]['minute']}")
                        continue
                    print('Buy at', date, date.strftime("%A"), data_list[i]['minute'])
                    print('Buy at', data_list[i]['close'], 'volume:', data_list[i]['volume'])
                    print(f'(Period max high: {max(self.max_high_period)} Period max volume: {max(self.max_volume_period)})')
                    print('..')
                    amount, hold_until_now, sold_price = self.risk_management(ticker, trade_date=date, traded_price=data_list[i]['close'], amount=balance, sell_ratio=sell_ratio, sell_to_keep=sell_to_keep)
                    if amount:
                        print(f"current amount: {round(amount, 2)}, amount ratio: {round(100 * (amount / balance - 1), 2)}")
                    if data_list[i]['close']:
                        self.get_last_price(data_list[i]['close'], ticker)
                    
                    if hold_until_now:
                        # self.balance_all += amount - self.balance_one
                        print(f'Current all balance: {self.balance_all} with {self.volatility} volatility')
                        return 
                    
                    updated_today = True
                    max_volume_today = max(data_list[i]['volume'], max_volume_today)

                    self.update_period(max_volume_today, high_today)

                    share = balance // data_list[i]['close']
                    balance += share * (sold_price - data_list[i]['close'])
                    print(f'current balance: {round(balance, 2)}')
                    print('---')

                if 'volume' in data_list[i] and data_list[i]['volume']:
                    max_volume_today = max(data_list[i]['volume'], max_volume_today)
                if 'high' in data_list[i] and data_list[i]['high']:
                    high_today = max(high_today, data_list[i]['high'])
            
            if not updated_today:
                self.update_period(max_volume_today, high_today)
        if amount != self.balance_one: 
            self.balance_all += amount - self.balance_one
            print(f'Current all balance: {round(self.balance_all, 2)} with {self.volatility} volatility')
        

    def update_period(self, max_volume_today, high_today):
        self.max_volume_period.popleft()
        self.max_volume_period.append(max_volume_today)
        self.max_high_period.popleft()
        self.max_high_period.append(high_today)

    def risk_management(self, ticker, trade_date, traded_price, amount, sell_ratio, sell_to_keep):
        """
        Return current amount, whether holding, price when and if sell
        """
        data_list = []
        leng = (datetime.now().date() - trade_date).days
        for i in range(1, leng + 1):
            date = trade_date + timedelta(i)
            if leng == 1 and date.weekday() >= 5:
                return self.last_day_price(ticker, trade_date, traded_price, amount)
            if date.weekday() >= 5:
                continue
            high = traded_price

            data_list = get_historical_intraday(ticker, date, token = token)
            for i in range(len(data_list)):
                if 'high' in data_list[i] and data_list[i]['high']:
                    high = max(high, data_list[i]['high'])
                # 1. 
                if 'close' in data_list[i] and data_list[i]['close'] and data_list[i]['close'] <= sell_ratio * traded_price:
                    self._sell_day, self._sell_minute = date, data_list[i]['minute']
                    amount = self.sell(amount=amount, trade_date=date, time=data_list[i]['minute'], sold_price=data_list[i]['close'], traded_price=traded_price, sell_ratio=sell_ratio)
                    
                    self.balance_all += amount - self.balance_one
                    return amount, False, data_list[i]['close']

                # 2. 
                # if 'close' in data_list[i] and data_list[i]['close'] and high > traded_price and (data_list[i]['close'] - traded_price) / (high - traded_price) < sell_to_keep:
                #     self._sell_day, self._sell_minute = date, data_list[i]['minute']
                #     amount = self.sell(amount=amount, trade_date=date, time=data_list[i]['minute'], sold_price=data_list[i]['close'], traded_price=traded_price, sell_ratio=sell_ratio, sell_by_lose=False)
                    
                #     self.balance_all += amount - self.balance_one
                #     return amount, False, data_list[i]['close']
        
        if data_list:
            x = 1
            while 'close' not in data_list[-x] or not data_list[-x]['close']:
                x += 1
            last_price = data_list[-x]['close']
            share = amount // traded_price
            amount += share * (last_price - traded_price)
            print('Hold it until now')
            # print(f'Hold it until now, current balance: {round(amount, 2)}')
            self.balance_all += amount - self.balance_one
            return amount, True, -1

        print('Just ordered')
        date = datetime.now().date()
        x = 1
        while not get_historical_intraday(ticker, date, token = self.token):
            date = (datetime.now() - timedelta(x)).date()
            x += 1
        last_price = get_historical_intraday(symbol=ticker, date=date, token = self.token)[-1]['close']
        share = amount // traded_price
        amount += share * (last_price - traded_price)
        self.balance_all += amount - self.balance_one
        return amount, True, -1

    def last_day_price(self, ticker, date, traded_price, amount):
        data_list = get_historical_intraday(ticker, date, token=self.token)
        x = 1
        while not data_list[-x]['close']:
            x += 1
        last_price = data_list[-x]['close']
        share = amount // traded_price
        amount += share * (last_price - traded_price)
        return amount, True, -1

    def sell(self, amount, trade_date, time, sold_price, traded_price, sell_ratio, sell_by_lose = True):
        share = amount // traded_price
        amount += share * (sold_price - traded_price)
        if sell_by_lose:
            print(f"Sell (lower than {100 * sell_ratio}% of buy price) - at {sold_price} @{trade_date} {time}")
            return amount
        else:
            print(f"Sell (keep 70% profit) - at {sold_price} @{trade_date} {time}")
            return amount
    
    def run(self, ticker_list, sell_ratio=0.94, alpha_volume=1.3, alpha_price=0.95, sell_to_keep=0.7):
        print('')
        print('Start new test...')
        print('')
        for ticker in ticker_list:
            self.clear()
            self.find_signal(ticker, sell_ratio=sell_ratio, alpha_volume=alpha_volume, alpha_price=alpha_price, sell_to_keep=sell_to_keep)
        print('')

    def clear(self):
        self.max_volume_period = deque([0 for i in range(self.period)])
        self.max_high_period = deque([0 for i in range(self.period)])
        self._sell_day = None
        self._sell_minute = None


# ticker_list = ['AMZN', 'TSLA', 'BABA', 'BA', 'FB', 
#                 'BA', 'SSNC', 'AEY', 'VXX', 'GOOG', 
#                 'BILI', 'ZTS', 'NFLX', 'CCL', 'QQQ']
# ticker_list = ['PQG', 'FLDM', 'MFH', 'SELB', 'ETTX']
# ticker_list = ['SURF', 'NNDM', 'BEAM', 'VRTX']
# ticker_list = ['BPYU', 'BHF', 'NRG', 'ARD', 'NLOK']
# ticker_list = ['QDEL', 'ZM', 'CRM', 'ACI', 'BERY', 
#                 'BPYU', 'BHF', 'NRG', 'ARD', 'NLOK']
# ticker_list = ['MRVL', 'GFI', 'CSIQ', 'XONE', 'BYSI']
# ticker_list = ['KIRK', 'OCUL', 'VNET', 'PLAN', 'GOGO',
#                 'SBUX', 'CRM', 'NIO', 'CYRX', 'ZSAN',
#                 'LINX', 'SWI', 'PBI', 'UFS', 'OIIM', 
#                 'PINS', 'ICLK']
# ticker_list = [ 'KIRK']
# ticker_list = ['MED', 'BABA', 'NXST', 'ABBV', 'NMIH',
                # 'PEAK', 'NEM', 'FB', 'BTI', 'UVE']
# ticker_list = ['VIRT', 'FUTU', 'PFSI', 'SPWH', 'DQ',
#                 'QDEL', 'EBS', 'GMAB', 'IPHI', 'BIG',
#                 'ENSG', 'VRTX', 'AMD', 'DT', 'NVDA', 
#                 'OLLI', 'GSX', 'DG', 'NFLX']
# ticker_list = ['UNH', 'MRK', 'NVS', 'TMO', 'CVS']
# ticker_list =['AAL', 'DIS', 'DAL', 'PLUG', 'NIO', 'GPRO', 'NCLH', 'SNAP']
# ticker_list = ['ACB', 'FIT', 'UAL', 'AMD', 'HEXO', 'T', 'CGC', 'NKLA', 'NVOA']
# ticker_list = ['GPRO', 'NIO', 'ZSAN', 'SWI', 'PBI',
#                 'BPYU', 'BHF', 'NRG', 'ARD', 'NLOK',
#                 'UNH', 'MRK', 'NVS', 'TMO', 'CVS',
#                 'AMZN', 'TSLA', 'BABA', 'BA', 'FB', 
#                 'SSNC', 'AEY', 'VXX', 'GOOG', 'BILI', 
#                 'ZTS', 'NFLX', 'CCL', 'QQQ', 'BAC',
#                   'GE', 'PFE', 'ALK', 'WRK']
# top_100_by_robinhood = ['AAPL', 'TSLA', 'F', 'GE', 'MSFT',
#                             'AAL', 'DIS', 'DAL', 'CCL', 'GPRO',
#                             'ACB', 'PLUG', 'NIO', 'BAC', 'NCLH',
#                             'SNAP', 'FIT', 'BA', 'MRNA', 'UAL']
# top_SP500 = ['MSFT', 'AAPL', 'FB', 'BRK.B', 'JNJ',
#                 'V', 'PG', 'JPM', 'UNH', 'MA',
#                 'INTC', 'VZ', 'HD', 'T', 'PFE',
#                 'MRK', 'PEP']
# penny_stock = ['CCNC', 'BIMI', 'KXIN', 'SMHI', 'LEE',
#                     'AIHS', 'ARC', 'EVK', 'MTSL', 'MFH', 
#                     'WEI', 'SOS', 'EYPT', 'FEDU', 'BBAR',
#                     'EARS', 'AIKI', 'LITB', 'ETTX']
# penny_stock = ['CCNC', 'BIMI', 'KXIN', 'SMHI', 'AIHS', 
#             'ARC', 'SOS', 'EYPT', 'BBAR',
#             'AIKI']
# mid-cap = ['YETI', 'FRPT', 'AAN', 'FIVE',
#                     'SMG', 'CNNE', 'GTN', 'VRNT', 
#                     'SMPL', 'BGCP', 'ENV', 'KW', 
#                     'HELE', 'BEP', 'GOOS']
if __name__ == "__main__":
    ticker_list = ['PDD']

    token = 'sk_17b078529ba34b7396ef93de2d19b287'
    test = BackTest(token=token, period=5, balance=8000, volatility=8)
    test.run(ticker_list)