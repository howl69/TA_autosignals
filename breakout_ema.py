import numpy as np
import pandas as pd
import time
from clickhouse_driver import Client
import logging
import datetime as dt
from telegram import Bot
from config import TICKERS, ema_n_days
from vars import CHANNEL_ID, bot_token, HOST

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.info("service started")

cur_ticker_cfg = {}
anom_vols = []
bot = Bot(bot_token)

def data_to_df(res):
    return pd.DataFrame(res, columns = ['tradedate','tradetime','secid','pr_open','pr_high','pr_low','pr_close','pr_std','vol','val','trades','pr_vwap','pr_change','trades_b','trades_s','val_b','val_s','vol_b','vol_s','disb','pr_vwap_b','pr_vwap_s','SYSTIME'])
    
def get_last_data():
    with Client(HOST, settings={"use_numpy": True}) as client:
        res = client.execute("SELECT * FROM algopack WHERE tradedate > today() - toIntervalDay(14)")
    cur_df = data_to_df(res)
    cur_df['date'] = pd.to_datetime(cur_df['tradedate'].astype(str) + ' ' + cur_df['tradetime'])
    cur_df = cur_df.set_index('date')
    return cur_df

def anom_vol_compute(n = 3, q = 0.75):
    with Client(HOST, settings={"use_numpy": True}) as client:
        res = client.execute("SELECT tradedate, tradetime, secid, vol FROM algopack")
    df = pd.DataFrame(res, columns=['tradedate', 'tradetime', 'secid', 'vol'])
    return df.groupby('secid')['vol'].rolling(n, min_periods=n).mean().dropna().groupby('secid').quantile(q)

def signal(item, cur_cfg, anom_vol, n_wait = 3, n_vol = 3):
    volumes = cur_cfg['volumes']
    if len(cur_cfg['volumes']) == n_vol:
        cur_cfg['volumes'][:-1] = cur_cfg['volumes'][1:]
        cur_cfg['volumes'][-1] = item['vol']
    else:
        cur_cfg['volumes'] = np.append(cur_cfg['volumes'], item['vol'])
    wait_flag = cur_cfg['wait_flag']
    up_flag = cur_cfg['up_flag']
    cur_i_candle = cur_cfg['cur_i_candle']
    mean_vol = volumes.mean()

    if wait_flag:
        if (up_flag and item['pr_vwap'] >= item['200EMA'])\
                or (not up_flag and item['pr_vwap'] < item['200EMA']):
            cur_i_candle += 1
        else:
            wait_flag = False
        
        if cur_i_candle == n_wait:
            if up_flag and item['pr_close'] / start_price < 1.01:
                cur_cfg['breaking_up_signal'] = item['pr_close']
            elif not up_flag and item['pr_close'] / start_price > 0.99:
                cur_cfg['breaking_down_signal'] = item['pr_close']
            wait_flag = False

    if not wait_flag:
        if (not up_flag and item['pr_vwap'] >= item['200EMA']) \
                or (up_flag and item['pr_vwap'] < item['200EMA']):
            up_flag = not up_flag
            if mean_vol > anom_vol:
                wait_flag = True
                start_price = item['pr_close']
                cur_i_candle = 0

    
    wait_flag = cur_cfg['wait_flag']
    up_flag = cur_cfg['up_flag']
    cur_i_candle = cur_cfg['cur_i_candle']

    return cur_cfg

def preprocessing(n_vol = 3, ema_n_days = 200):
    global anom_vols
    global cur_ticker_cfg

    anom_vols = anom_vol_compute()
    cur_df = get_last_data()

    for ticker in TICKERS:
        ticker_df = cur_df.query(f"secid == '{ticker}'").copy()
        ticker_df[f'{ema_n_days}EMA'] = ticker_df['pr_close'].ewm(span=ema_n_days, min_periods=ema_n_days, adjust=False, ignore_na=False).mean()
        dates = ticker_df.index
        cur_cfg = {
            'wait_flag': False,
            'up_flag': float(ticker_df.loc[dates[-1],f'{ema_n_days}EMA']) <= float(ticker_df.loc[dates[-1],'pr_vwap']),
            'volumes': ticker_df.loc[dates[-n_vol:], 'vol'], 
            'cur_i_candle': 0
        }
        cur_ticker_cfg[ticker] = cur_cfg

def new_data_processing(ticker_df, ticker, ema_n_days = 200):
    ticker_df[f'{ema_n_days}EMA'] = ticker_df['pr_close'].ewm(span=ema_n_days, min_periods=ema_n_days, adjust=False, ignore_na=False).mean()
    cur_cfg = cur_ticker_cfg[ticker]
    cur_cfg['breaking_up_signal'] = np.nan
    cur_cfg['breaking_down_signal'] = np.nan

    cur_cfg = signal(ticker_df.loc[ticker_df.index[-1]], cur_cfg, anom_vols[ticker])
    cur_ticker_cfg[ticker] = cur_cfg

    if not np.isnan(cur_cfg['breaking_up_signal']):
        bot.send_message(CHANNEL_ID, f"{ticker} - breaking_up_signal")
        logger.info(f"{ticker} - breaking_up_signal")
    elif not np.isnan(cur_cfg['breaking_down_signal']):
        bot.send_message(CHANNEL_ID, f"{ticker} - breaking_down_signal")
        logger.info(f"{ticker} - breaking_down_signal")

def main_process(ema_n_days = 200):
    preprocessing()

    latest_date = dt.datetime(year = 2023, month=1, day=1)
    while(True):
        data = get_last_data()
        new_date = max(data.index)

        if latest_date < new_date:  
            for ticker in TICKERS:
                ticker_df = data.query(f"secid == '{ticker}'").copy()
                new_data_processing(ticker_df, ticker, ema_n_days)
            
        
        latest_date = new_date
        logger.info('sleep')
        time.sleep(10)

if __name__ == '__main__':
    main_process(ema_n_days)
