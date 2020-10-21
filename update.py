from collections import deque
import numpy as np
from iexfinance.stocks import get_historical_intraday
from datetime import datetime, timedelta
import datetime as dt
import pickle

class UpdateData(object):
    def __init__(self, token, pickle_file):
        self._data = {}
        self.token = token
        self.file_name = pickle_file
    
    def get_volume_list_today(self, data_list):
        slot = len(data_list) // 15
        slot_plus = len(data_list) % 15
        slot_today = []
        for i in range(slot):
            slot_each = 0
            count = 0
            for j in range(15):
                idx = 15 * i + j
                if 'volume' in data_list[idx] and data_list[idx]['volume']:
                    if data_list[idx]['volume'] > 0:
                        count += 1
                        slot_each += data_list[idx]['volume']
            if count > 0:
                slot_each *= (15 / count)
            slot_today.append(slot_each)

        if slot_plus > 0:
            slot_each = 0
            count = 0
            for j in range(slot_plus):
                idx = slot * 15 + j
                if 'volume' in data_list[idx] and data_list[idx]['volume']:
                    if data_list[idx]['volume'] > 0:
                        count += 1
                        slot_each += data_list[idx]['volume']
            if count > 0:
                slot_each *= (15 / count)
            slot_today.append(slot_each)

        return slot_today

    def get_high_list_today(self, data_list):
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

    def get_price_close_open(self, data_list):
        start, end = 0, -1
        try:
            while not ('close' in data_list[end] and data_list[end]['close']):
                end -= 1
            while not ('open' in data_list[start] and data_list[start]['open']):
                start += 1

            return data_list[end]['close'] - data_list[start]['open']
        except:
            return None
    
    def get_volume_close_open(self, data_list):
        start, end = 0, -1
        try:
            while not ('volume' in data_list[end] and data_list[end]['volume']):
                end -= 1
            while not ('volume' in data_list[start] and data_list[start]['volume']):
                start += 1

            return data_list[end]['volume'] - data_list[start]['volume']
        except:
            return None

    def init_data(self, ticker, midnight):
        print(f'{ticker} initializes ..')
        high_30_days = []
        volume_30_days = []

        start_date = datetime.now()
        if midnight:
            start_date = start_date - timedelta(1)

        for i in reversed(range(29)):
            date = (start_date - timedelta(i)).date()
            if date.weekday() >= 5:
                continue
            
            data_list = get_historical_intraday(ticker, date, token=self.token)
            high_today = self.get_high_list_today(data_list)
            volume_today = self.get_volume_list_today(data_list)
            if data_list and high_today and volume_today:
                high_30_days.append(max(high_today))
                volume_30_days.append(max(volume_today))
        
        today_data_list = get_historical_intraday(ticker, start_date, token=self.token)
        price_close_open = self.get_price_close_open(today_data_list)
        volume_close_open = self.get_volume_close_open(today_data_list)

        if not self._data:
            self._data = {ticker: {'volume': volume_30_days,
                                    'high': high_30_days,
                                    'date': start_date.date(),
                                    'price_close_open': price_close_open,
                                    'volume_close_open': volume_close_open
                                    }}
        else:
            self._data.update({ticker: {'volume': volume_30_days,
                                    'high': high_30_days,
                                    'date': start_date.date(),
                                    'price_close_open': price_close_open,
                                    'volume_close_open': volume_close_open
                                    }})

    def save_date(self):
        with open(self.file_name, 'wb') as f:
            pickle.dump(self._data, f, pickle.HIGHEST_PROTOCOL)

    def load_data(self):
        with open(self.file_name, "rb") as f:
            objects = []
            while True:
                try:
                    objects.append(pickle.load(f))
                except EOFError:
                    break
        self._data = objects[0]

    def update(self, ticker, midnight):
        if ticker not in self._data.keys():
            self.init_data(ticker, midnight)
        else:
            self.update_data(ticker, midnight)

    def update_data(self, ticker, midnight):
        print(f'{ticker} updates ..')
        start_date = datetime.now().date()
        if midnight:
            start_date = (start_date - timedelta(1)).date()

        updated_date = self._data[ticker]['date']

        if updated_date == start_date:
            print(f'{ticker} already up-to-date')
            return
        
        days_delta = (start_date - updated_date).days
        for i in range(days_delta):
            date = updated_date + timedelta(i + 1)
            if date.weekday() >= 5:
                continue

            data_list = get_historical_intraday(ticker, date, token=self.token)
            high_today = self.get_high_list_today(data_list)
            volume_today = self.get_volume_list_today(data_list)
            if data_list and high_today and volume_today:
                self._data[ticker]['high'].append(max(high_today))
                self._data[ticker]['volume'].append(max(volume_today))

            self._data[ticker]['date'] = date
            self._data[ticker]['price_close_open'] = self.get_price_close_open(data_list)
            self._data[ticker]['volume_close_open'] = self.get_volume_close_open(data_list)

    def clean_data(self):
        if not self._data:
            return

        remove_list = []
        for ticker in self._data.keys():
            if sum([i > 0 for i in self._data[ticker]['volume']]) < 5 or sum([i > 0 for i in self._data[ticker]['high']]) < 5:
            # if not all(i > 0 for i in self._data[ticker]['volume']) or not all(i > 0 for i in self._data[ticker]['high']):
                remove_list.append(ticker)
                print(f'removed {ticker}')

        if not remove_list:
            return
        for ticker in remove_list:
            self._data.pop(ticker)

    def run(self, ticker_list, midnight=False):
        print('')
        print('load data ..')
        self.load_data()
        print('')

        for ticker in ticker_list:
            try:
                self.update(ticker, midnight)
                print('Done')
                print('')  
            except:
                print(f'{ticker} fails to update')
                print('')
                pass
        if self._data:
            self.clean_data()
            self.save_date()
            print('cleaned')
        print(f'data saved - {len(self._data)} stocks')
        

if __name__ == "__main__":

    ticker_list = ['CCNC', 'PEP', 'YEYI',
                    'MRNA','ICLK', 'MRVL', 'HD', 'SWI', 'LITB',
                    'NIO', 'OLLI', 'PEAK', 'XONE', 'UNH', 'BEP', 
                    'BERY', 'DT', 'ARC', 'PFE', 'EVK', 'PQG',
                    'NEM', 'NMIH', 'ZTS', 'BILI', 'GSX', 
                    'SBUX', 'BBAR', 'BAC', 'BYSI', 'GFI','ACB', 
                    'QDEL', 'NNDM', 'SELB', 'VZ', 'PINS', 'FB', 
                    'UAL', 'FEDU', 'VNET', 'FIVE', 'GE',
                    'OIIM', 'JPM', 'SURF', 'PLAN', 'SMHI', 'CSIQ', 
                    'BEAM', 'EBS', 'KW', 'AMD', 'TMO', 'GMAB', 
                    'AAN', 'VRTX', 'ACI', 'PG', 'AIHS', 
                    'AEY', 'BABA', 'HEXO', 'T',
                    'SOS', 'MED', 'GOOS', 'FIT', 'V', 'NRG', 
                    'NVDA', 'ARD', 'ABBV', 'BTI', 'PBI', 'VIRT',
                    'INTC', 'NKLA', 'CVS', 'KXIN', 'MA', 'SMG', 
                    'ENV', 'BGCP', 'DAL', 'MTSL', 'VRNT', 'NVOA',
                    'ETTX', 'BIG', 'AIKI', 'NVS', 'BHF', 'EARS',
                    'DG', 'GPRO', 'NCLH', 'IPHI', 'FRPT', 'NFLX',
                    'CCL', 'CGC', 'NXST', 'ENSG', 'FLDM', 'CRM',
                    'JNJ', 'GOGO', 'AAL', 'HELE', 'ZSAN',
                    'OCUL', 'LINX', 'DQ', 'SSNC', 'BA', 'GOOG',
                    'MRK', 'LEE', 'VXX', 'BPYU', 'UFS', 'BIMI',
                    'CNNE', 'FUTU', 'WEI', 'EYPT', 'SPWH', 'SMPL',
                    'ALK', 'GTN', 'CYRX', 'PFSI', 'UVE', 'SNAP',
                    'NLOK', 'BRK.B', 'MFH', 'DIS', 'QQQ', 'ZM',
                    'PLUG', 'KIRK', 'MCD', 'NVO', 'COST', 'DHR', 'AVGO',
                    'UPS', 'AMGN', 'TMUS', 'NTES', 'BIDU',
                    'LFC', 'CEA', 'NOAH', 'LI', 'NIU',
                    'NIO', 'PDD', 'TCOM', 'TIGR', 'JD',
                    'XPENG', 'GS', 'C', 'BAC', 'JPM',
                    'AMZN', 'MS', 'SPG', 'BS', 'WFC',
                    'PEP', 'JNJ', 'DILD', 'MCD', 'GSK',
                    'WMG', 'GM', 'TM', 'HMC', 'TSLA',
                    'GE', 'NKE', 'WMT', 'PG', 'BA',
                    'MMM', 'FDX', 'HLT', 'ERX',
                    'SPY', 'BABA',  'FB', 'SSNC', 'AEY', 
                    'VXX', 'GOOG','BILI', 'ZTS', 'NFLX', 'CCL',
                    'IBM', 'BYD', 'LOGI', 'CCK', 
                    'BLI', 'HXL', 'PACB', 'CRSR',
                    'NNOX', 'OSUR', 'HYLN', 'IH', 'ACB',
                    'UXIN', 'KXIN', 'AMC','AAPL', 'F', 
                    'GE', 'MSFT', 'AAL', 
                    'DIS', 'DAL', 'NCLH', 'SNAP', 'FIT',
                    'MRNA', 'UAL', 'AMD', 'HEXO', 'T', 'NFLX', 'CGC',
                    'UBER', 'TWTR', 'NKLA', 'KO', 'NVDA',
                    'APHA', 'CRON', 'ZNGA', 'RCL', 'SBUX',
                    'PFE', 'LUV''INO', 'SAVE', 'MRO', 'JBLU', 'WKHS',
                    'SPCE', 'XOM', 'VOO', 'WMT', 'MGM',
                    'DKNG', 'HTZ', 'NOK', 'SNE', 'GUSH',
                    'SQ', 'GM', 'MFA', 'SIRI', 'PTON',
                    'IVR', 'ZM', 'USO', 'GOOGL', 'UCO',
                    'CPRX', 'WORK', 'NRZ', 'PENN',
                    'INTC', 'TLRY', 'V', 'PSEC', 'BRK.B',
                    'WFC', 'JNJ', 'FCEL', 'LYFT', 'SPHD',
                    'KOS', 'RKT', 'IBIO', 'PYPL', 'ET',
                    'PLAY', 'SRNE', 'GILD', 'KODK', 'HAL',
                    'TXMD', 'BP', 'VTI', 'BYND',
                    'PLTR', 'NVAX', 'PG', 'HD', 'UNH', 'VZ', 'CMCSA',
                    'PEP', 'ABT', 'ORCL', 'TMO', 'MCD',
                    'NVO', 'COST', 'DHR', 'AVGO', 'UPS',
                    'AMGN', 'TMUS', 'SQ', 'MED',
                    'BABA', 'NXST', 'ABBV', 'NMIH', 'PEAK',
                    'NEM', 'BTI', 'FB', 'UVE', 'BPYU',
                    'BHF', 'NRG', 'ARD', 'NLOK',
                    'QDEL', 'CRM', 'ACI', 'BERY', 'LVGO',
                    'IMMU', 'YETI', 'CVX', 'WBA', 'HON',
                    'DBX', 'NOVA', 'WFH', 'DNB', 
                    'LMDN', 'IPOS', 'RPRX', 'ADBE', 
                    'DOCU', 'EBAY', 'ETSY', 'STNE', 'EXAS',
                    'LAUR', 'LEVI', 'PE', 'TDS', 'USM', 
                    'YETI', 'FRPT', 'FNF', 'CDAY','NMRK', 
                    'VKTX', 'CL=F', 'OXY', 'SNOW', 'ARKW', 
                    'ITOT', 'XPEV', 'ARKK', 'BTE']
    ticker_list = ['WDFC', 'GSX', 'CRSP', 'ERIC', 'CHU', 'FVRR', 'PHR', 'CMBM', 'ESTA', 'DAO']
    # ticker_list = ['BYD' ]
    # Do NOT run this script during HOURS!!
    # If you're runing after 12 am to update, use True in midnight parameter

    token = 'sk_17b078529ba34b7396ef93de2d19b287'
    update = UpdateData(token, 'data/data.pickle')
    update.run(ticker_list, midnight=True)