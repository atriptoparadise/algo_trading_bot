from collections import deque
import numpy as np
from iexfinance.stocks import get_historical_intraday
import alpaca_trade_api as tradeapi
from datetime import datetime, timedelta
import datetime as dt
import pickle
from config import *


class UpdateData(object):
    def __init__(self, token, pickle_file):
        self._data = {}
        self.token = token
        self.file_name = pickle_file
        self.api = tradeapi.REST(API_KEY, SECRET_KEY, api_version = 'v2')

    def save_data(self):
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

    def get_price_close_open(self, data_list):
        start, end = 0, -1
        try:
            while not ('close' in data_list[end] and data_list[end]['close']):
                end -= 1
            while not ('open' in data_list[start] and data_list[start]['open']):
                start += 1

            diff = data_list[end]['close'] - data_list[start]['open']
            if data_list[start]['open'] == 0:
                return diff, None
            return diff, 100 * (diff / data_list[start]['open'])
        except:
            return None, None
    
    def get_volume_close_open(self, data_list):
        start, end = 0, -1
        try:
            while not ('volume' in data_list[end] and data_list[end]['volume']):
                end -= 1
            while not ('volume' in data_list[start] and data_list[start]['volume']):
                start += 1

            diff = data_list[end]['volume'] - data_list[start]['volume']
            if data_list[start]['volume'] == 0:
                return diff, None
            return diff, 100 * (diff / data_list[start]['volume'])
        except:
            return None, None

    def get_high_volume_day(self, ticker, date):
        data = self.api.get_barset(ticker, '15Min', limit = 1000).df.reset_index()
        data['date'] = [i.date() for i in data.time]
        return max(data[data.date == date].iloc[:, 2]), max(data[data.date == date].iloc[:, -2])

    def get_volume_list(self, ticker):
        data = self.api.get_barset(ticker, '15Min', limit = 1000).df.reset_index()
        data['date'] = [i.date() for i in data.time]
        time = data.date.unique()
        volume_max = []
        for i in time:
            if self.midnight == False:
                volume_max.append(data[data.date == i].iloc[:, -2].max())
            else:
                if i != datetime.now().date():
                    volume_max.append(data[data.date == i].iloc[:, -2].max())
        return volume_max
    
    def get_high_list(self, ticker):
        data = self.api.get_barset(ticker, '15Min', limit = 1000).df.reset_index()
        data['date'] = [i.date() for i in data.time]
        time = data.date.unique()
        high_max = []
        for i in time:
            if self.midnight == False:
                high_max.append(data[data.date == i].iloc[:, 2].max())
            else:
                if i != datetime.now().date():
                    high_max.append(data[data.date == i].iloc[:, 2].max())
        return high_max

    def init_data(self, ticker):
        print(f'{ticker} initializes ..')

        start_date = datetime.now().date()
        if self.midnight:
            start_date = start_date - timedelta(1)
        while start_date.weekday() >= 5:
            start_date = start_date - timedelta(1)

        today_data_list = get_historical_intraday(ticker, start_date, token=self.token)
        price_close_open, price_ratio = self.get_price_close_open(today_data_list)
        volume_close_open, volume_ratio = self.get_volume_close_open(today_data_list)
        high_30_days = self.get_high_list(ticker)
        volume_30_days = self.get_volume_list(ticker)

        if not self._data:
            self._data = {ticker: {'volume': volume_30_days,
                                    'high': high_30_days,
                                    'date': start_date.date(),
                                    'price_close_open': price_close_open,
                                    'volume_close_open': volume_close_open,
                                    'price_ratio': price_ratio,
                                    'volume_ratio': volume_ratio
                                    }}
        else:
            self._data.update({ticker: {'volume': volume_30_days,
                                    'high': high_30_days,
                                    'date': start_date,
                                    'price_close_open': price_close_open,
                                    'volume_close_open': volume_close_open,
                                    'price_ratio': price_ratio,
                                    'volume_ratio': volume_ratio
                                    }})
        
    def update_data(self, ticker):
        print(f'{ticker} updates ..')
        start_date = datetime.now().date()
        if self.midnight:
            start_date = start_date - timedelta(1)

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
            high_today, volume_today = self.get_high_volume_day(ticker, date)

            self._data[ticker]['high'].append(high_today)
            self._data[ticker]['volume'].append(volume_today)
            self._data[ticker]['date'] = date
            self._data[ticker]['price_close_open'], self._data[ticker]['price_ratio'] = self.get_price_close_open(data_list)
            self._data[ticker]['volume_close_open'], self._data[ticker]['volume_ratio'] = self.get_volume_close_open(data_list)

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

    def update(self, ticker):
        if ticker not in self._data.keys():
            self.init_data(ticker)
        else:
            self.update_data(ticker)

    def run(self, watch_list, midnight=False):
        print('')
        print('load data ..')
        self.load_data()
        print('')
        self.midnight = midnight

        for ticker in watch_list:
            try:
                self.update(ticker)
                print('Done')
                print('')  
            except:
                print(f'{ticker} fails to update')
                print('')
                pass
        
        if self._data:
            self.clean_data()
            self.save_data()
            print('cleaned')
        print(f'data saved - {len(self._data)} stocks')


if __name__ == "__main__":
    watch_list = ['WEI', 'CCNC', 'PEP', 'MRNA', 'ICLK', 'MRVL', 'HD', 'SWI', 'LITB', 'NIO', 'OLLI', 'PEAK', 'XONE', 'UNH', 'BEP', 'BERY', 'DT', 'ARC', 'PFE', 'EVK', 'PQG', 'NEM', 'NMIH', 'ZTS', 'BILI', 'GSX', 'SBUX', 'BBAR', 'BAC', 'BYSI', 'GFI', 'ACB', 'QDEL', 'NNDM', 'SELB', 'VZ', 'PINS', 'FB', 'UAL', 'FEDU', 'VNET', 'FIVE', 'GE', 'OIIM', 'JPM', 'SURF', 'PLAN', 'SMHI', 'CSIQ', 'BEAM', 'EBS', 'KW', 'AMD', 'TMO', 'GMAB', 'AAN', 'VRTX', 'ACI', 'PG', 'AIHS', 'AEY', 'BABA', 'HEXO', 'T', 'SOS', 'MED', 'GOOS', 'FIT', 'V', 'NRG', 'NVDA', 'ARD', 'ABBV', 'BTI', 'PBI', 'VIRT', 'INTC', 'NKLA', 'CVS', 'KXIN', 'MA', 'SMG', 'ENV', 'BGCP', 'DAL', 'MTSL', 'VRNT', 'ETTX', 'BIG', 'AIKI', 'NVS', 'BHF', 'EARS', 'DG', 'GPRO', 'NCLH', 'IPHI', 'FRPT', 'NFLX', 'CCL', 'CGC', 'NXST', 'ENSG', 'FLDM', 'CRM', 'JNJ', 'GOGO', 'AAL', 'HELE', 'ZSAN', 'OCUL', 'LINX', 'DQ', 'SSNC', 'BA', 'GOOG', 'MRK', 'LEE', 'VXX', 'BPYU', 'UFS', 'BIMI', 'CNNE', 'FUTU', 'EYPT', 'SPWH', 'SMPL', 'ALK', 'GTN', 'CYRX', 'PFSI', 'UVE', 'SNAP', 'NLOK', 'BRK.B', 'MFH', 'DIS', 'ZM', 'PLUG', 'KIRK', 'MCD', 'NVO', 'COST', 'DHR', 'AVGO', 'UPS', 'AMGN', 'TMUS', 'NTES', 'BIDU', 'LFC', 'CEA', 'NOAH', 'LI', 'NIU', 'PDD', 'TCOM', 'TIGR', 'JD', 'GS', 'C', 'AMZN', 'MS', 'SPG', 'WFC', 'GSK', 'WMG', 'GM', 'TM', 'HMC', 'TSLA', 'NKE', 'WMT', 'MMM', 'FDX', 'HLT', 'ERX', 'SPY', 'IBM', 'LOGI', 'CCK', 'BLI', 'HXL', 'PACB', 'CRSR', 'NNOX', 'OSUR', 'HYLN', 'IH', 'UXIN', 'AMC', 'AAPL', 'F', 'MSFT', 'UBER', 'TWTR', 'KO', 'APHA', 'CRON', 'ZNGA', 'RCL', 'SAVE', 'MRO', 'JBLU', 'WKHS', 'SPCE', 'XOM', 'VOO', 'MGM', 'DKNG', 'HTZ', 'NOK', 'SNE', 'GUSH', 'SQ', 'MFA', 'SIRI', 'PTON', 'IVR', 'USO', 'GOOGL', 'UCO', 'CPRX', 'WORK', 'NRZ', 'PENN', 'TLRY', 'PSEC', 'FCEL', 'LYFT', 'SPHD', 'KOS', 'RKT', 'IBIO', 'PYPL', 'ET', 'PLAY', 'SRNE', 'GILD', 'KODK', 'HAL', 'TXMD', 'BP', 'VTI', 'BYND', 'PLTR', 'NVAX', 'CMCSA', 'ABT', 'ORCL', 'LVGO', 'IMMU', 'YETI', 'CVX', 'WBA', 'HON', 'DBX', 'NOVA', 'WFH', 'DNB', 'IPOS', 'RPRX', 'ADBE', 'DOCU', 'EBAY', 'ETSY', 'STNE', 'EXAS', 'LAUR', 'LEVI', 'PE', 'TDS', 'USM', 'FNF', 'CDAY', 'NMRK', 'VKTX', 'OXY', 'SNOW', 'ARKW', 'ITOT', 'XPEV', 'ARKK', 'BTE', 'BYD', 'WDFC', 'CRSP', 'ERIC', 'CHU', 'FVRR', 'PHR', 'CMBM', 'ESTA', 'DAO', 'ADAP', 'LAC', 'HAE', 'WGO', 'IRBT', 'SMTS', 'BGI', 'SQBG', 'ALGN', 'MXL', 'REPL', 'SDC', 'RPTX', 'GME', 'BCLI', 'TC', 'TAL', 'CINF', 'GPS', 'CNX', 'SABR', 'GLBS', 'IPOB', 'HA', 'AMSC', 'ATVI', 'AYX', 'APPN', 'HQY', 'BL', 'ESTC', 'WU', 'FANG', 'EVK', 'TC', 'KBSF', 'SQBG', 'ALGN', 'BPTH', 'BEAM', 'IH', 'CDOR', 'NVFY', 'HX', 'NOA', 'HA', 'EVK', 'TC', 'KBSF', 'SQBG', 'ALGN', 'BPTH', 'CCM', 'BEAM', 'IH', 'CDOR', 'NVFY', 'HX', 'NOA', 'HA', 'SABR', 'LMPX', 'CGA', 'SQNS', 'INBK', 'DPST', 'NTN', 'GPS', 'OTRK', 'MXL', 'LXU', 'ENLC', 'ARD', 'ARRY', 'RPTX', 'CNX', 'CLPS', 'CRDF', 'RBET', 'RCI', 'WBS', 'VIAO', 'PXLW', 'NRGU', 'IMTE', 'IKNX', 'GUSH', 'TALO','SIEN', 'OIIM', 'DMTK', 'ARTW', 'HIBL', 'PSTX', 'NCNA', 'AGIO', 'SNOW', 'BEST', 'ESTA', 'BKD', 'ADCT', 'SANW', 'NNDM', 'SAVE', 'DDD', 'BNKU', 'PACW', 'NCTY', 'CCB', 'FOSL', 'DFS', 'SNA', 'SWN', 'CATB', 'VET', 'EXPE', 'JWN', 'JBLU', 'STL', 'NBLX', 'CAL', 'SMMF', 'STKS', 'GDP', 'SIX', 'NGL', 'LIND', 'UMC', 'LOGC', 'AXL', 'MR', 'PINE', 'UCBI', 'ALLK', 'GB', 'RRC', 'LVS', 'KRON', 'INTZ', 'STTK', 'INBX', 'PHAT', 'GO', 'CUTR', 'IAF', 'CGC', 'NESR', 'GTIM', 'FFWM', 'NID', 'TREE', 'MGEE', 'WEC', 'CLTL', 'AEIS', 'ATR', 'MDU', 'GEF', 'GUT''DTP', 'DD', 'FUL', 'GEF.B', 'TTEC', 'YUM', 'DLHC', 'DLB', 'KF', 'BBU', 'MITK', 'KWR', 'UGI', 'EVV', 'BLX', 'LDL', 'MCHP', 'AMH', 'TW', 'NPO', 'CDXS', 'SIM', 'NGG', 'EVY', 'LPLA', 'ENV', 'UTG', 'NTNX', 'KO', 'HNGR', 'DVA', 'HAS', 'PROF', 'PORM', 'CKH', 'LXU', 'FL', 'NMCI', 'ES', 'IIIN', 'YORW', 'MU', 'SIRI', 'LNT', 'AEE', 'ETR', 'STAG', 'LARK', 'CNET', 'ALB', 'DTE', 'MMYT', 'HIMX', 'CAF', 'UTF', 'AA', 'ESE', 'PCH', 'ST', 'GGB', 'WRB', 'BR', 'HLIT', 'AY', 'SFBS', 'QUOT', 'ORBC', 'PSMT', 'KEYS', 'RNR', 'BANC', 'ZNTL', 'GOED', 'BERY', 'CYH', 'KRON', 'INTZ', 'STTK', 'INBX', 'MAACU', 'QELLU', 'FGNA.U', 'EPP', 'CUTR', 'CMLFU', 'XME', 'RWL', 'SCKT', 'STRT', 'SCKT', 'AAL', 'F', 'AMD', 'YGYI', 'XLF', 'ZDGE', 'GNUS', 'MARK', 'LLNW', 'EEM', 'ITRM', 'GILD', 'MRO', 'VALE', 'HTZ', 'ITUB', 'MAT', 'XOM', 'BBD', 'BVXY', 'ABEV', 'XLE', 'WFC', 'NAKD', 'M', 'DKNG', 'JKS', 'SPXS', 'NCLH', 'CLF', 'X', 'AKBA', 'GPOR', 'EWZ', 'JMIA', 'IWM', 'HYG', 'UAL', 'UVXY', 'FSLY', 'OXY', 'EFA', 'CLNY', 'SLV', 'IEMG', 'PBR', 'PINS', 'FCX', 'GPS', 'KMI', 'MU', 'CLPS', 'RIG', 'NOK', 'CSCO', 'BBI', 'BP', 'COP', 'ZNGA', 'BIMI', 'TWTR', 'SWN', 'SPXU', 'GDX', 'SNDL', 'SLB', 'IAU', 'SLDB', 'JBLU']

    update = UpdateData(TOKEN, 'data/data.pickle')
    update.run(watch_list, midnight=False)