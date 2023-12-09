import mplfinance as mpf
import numpy as np
from telegram import Bot
import datetime as dt
import pandas as pd
from config import CHANNEL_ID, bot_token

bot = Bot(bot_token)

def ema_sig_plot_send(ticker_df, ticker, sig_type, ema_n_days = 200, plot_size = 500):
    ticker_df['date'] = ticker_df['tradedate'] + ' ' + ticker_df['tradetime']
    ticker_df.index = pd.DatetimeIndex(ticker_df['date'])

    if f"{ema_n_days}EMA" not in ticker_df.columns:
        ticker_df['200EMA'] = ticker_df['pr_close'].ewm(span=200, min_periods=200, adjust=False, ignore_na=False).mean()
    
    plot_df = ticker_df.loc[ticker_df.index[-plot_size:]]
    sig = np.ones(plot_size)*np.nan
    sig[-1] = plot_df.loc[plot_df.index[-1], 'pr_close']
    if sig_type == 'breakout_up':
        color = 'green'
    else:
        color = 'red'

    apds = [
        mpf.make_addplot(plot_df['200EMA'],type='line',color='blue'),
        mpf.make_addplot(sig, type='scatter', markersize=30, marker = 'o', color=color)

    ]
    direction = 'вверх' if sig_type == 'breakout_up' else 'вниз'
    text = f'#{ticker}\nПробили EMA200 {direction} в <a href="https://www.tinkoff.ru/invest/stocks/{ticker.upper()}?utm_source=security_share">{ticker.upper()}</a>'
    t = dt.datetime.strftime(dt.datetime.today(), "%Y-%m-%dT%H%M%S")
    path = f'ema_{ticker}_{sig_type}_{t}.png'
    mpf.plot(plot_df, 
            type='candle', 
            columns=['pr_open', 'pr_high', 'pr_low', 'pr_close', 'vol'], 
            volume = True,
            addplot=apds,
            style='yahoo',
            savefig=path
            )
    
    bot.send_photo(CHANNEL_ID, open(path,'rb'), caption = text, parse_mode='html')
