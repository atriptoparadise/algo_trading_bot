import pandas as pd
import requests, json
from datetime import datetime
from pandas.tseries.offsets import BDay
from config import *


col_list = {0: 'close', 
            1: 'next_1_day_close', 
            2: 'next_2_days_close', 
            5: 'next_1_week_close'}

def reset_idx():
    df = pd.read_csv('data/signals.csv')
    df = df.iloc[:, 1:]
    df.to_csv('data/signals.csv')

def update_close(day, df):
    today = datetime.now().date()
    col = col_list[day]
    col_ratio = col + '_ratio'

    for idx in range(df.shape[0]):
        if df.loc[idx, col]:
            continue
        if day == 0 and df.loc[idx, 'after_3_pm'] == 1:
            continue
        
        ticker = df.iloc[idx, 0]
        trade_date = datetime.strptime(df.iloc[idx, 1], '%Y/%m/%d').date()
        days_delta = (today - trade_date).days

        if days_delta > day - 1:
            date = trade_date + BDay(day)
            date = date.strftime('%Y-%m-%d')
            response = requests.get(f'{POLY_URL}/v1/open-close/{ticker}/{date}?apiKey={API_KEY}')
            content = json.loads(response.content)

            if 'close' in content and content['close']:
                df.loc[idx, col] = content['close']
                df.loc[idx, col_ratio] = (content['close'] / df.loc[idx, 'entry_price'] - 1) * 100

    return df

def update_ratio(day, df):
    col = col_list[day]
    col_ratio = col + '_ratio'
    
    for idx in range(df.shape[0]):
        if df.loc[idx, col] and df.loc[idx, col] > 0 and not df.loc[idx, col_ratio]:
            df.loc[idx, col_ratio] = (df.loc[idx, col] / df.loc[idx, 'entry_price'] - 1) * 100
    
    return df

def update_all():
    reset_idx()
    df = pd.read_csv('data/signals.csv', index_col=0)
    df = df.where(pd.notnull(df), None)
    day_list = [0, 1, 2, 5]

    for day in day_list:
        df = update_close(day, df)
        print(f'{day} day(s) completed')

    df.to_csv('data/signals.csv')

if __name__ == "__main__":
    update_all()