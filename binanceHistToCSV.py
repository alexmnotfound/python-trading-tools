import pandas as pd
import requests
import os
from datetime import datetime
import tickers as tk


def historicData(symbol, interval='1d', startTime=None, endTime=None, limit=1000):
    """
        Getting historic Data from Binance API

    :param symbol: ticker (BTCUSDT, ETHUSDT, etc..)
    :param interval:
        Minutes: 1m, 2m, 3m, 15m, 30m
        Hours: 1h, 2h, 4h
        Days: 1d, 3d
        Month: 1M
    :param startTime: time in ms
    :param endTime: time in ms
    :param limit: row limits (1000 default)
    :return: DataFrame with OHLC price history
    """

    url = 'https://api.binance.com/api/v3/klines'

    params = {'symbol': symbol, 'interval': interval,
              'startTime': startTime, 'endTime': endTime, 'limit': limit}

    r = requests.get(url, params=params)
    js = r.json()

    # Creating Dataframe
    cols = ['openTime', 'Open', 'High', 'Low', 'Close', 'Volume', 'cTime',
            'qVolume', 'trades', 'takerBase', 'takerQuote', 'Ignore']

    df = pd.DataFrame(js, columns=cols)

    # Converting strings to numeric
    df = df.apply(pd.to_numeric)

    # Timestamp Index handling
    df.index = pd.to_datetime(df.openTime, unit='ms')

    # Dropping unused columns
    df = df.drop(['openTime', 'cTime', 'takerBase', 'takerQuote', 'Ignore'], axis=1)
    df = df.drop(['Volume', 'qVolume', 'trades'], axis=1)

    return df


def dateToMs(date):
    """
    Cambia fecha a MS
    :param date: str(AAAA-MM-DD)
    :return: ms
    """
    dt_obj = datetime.strptime(f'{date} 00:00:00',
                               '%Y-%m-%d %H:%M:%S')

    millisec = int(dt_obj.timestamp() * 1000) - (3600000*3) #UTC-3

    return millisec


def main():

    tickers = tk.tickers
    #tickers = ('BTCUSDT', 'ETHUSDT')
    timeframe = '1h'

    months = (
        ('2021-01-01', '2021-01-31'),
        ('2021-02-01', '2021-02-28'),
        ('2021-03-01', '2021-03-31'),
        ('2021-04-01', '2021-04-30'),
        ('2021-05-01', '2021-05-31'),
        ('2021-06-01', '2021-06-30'),
        ('2021-07-01', '2021-07-31'),
        ('2021-08-01', '2021-08-31'),
        ('2021-09-01', '2021-09-30'),
        ('2021-10-01', '2021-10-31'),
        ('2021-11-01', '2021-11-30'),
        ('2021-12-01', '2021-12-31'),
    )

    for ticker in tickers:

        db = pd.DataFrame()

        print(f'Descargando historial de {ticker}')

        for month in months:
            fromDate = month[0]
            toDate = month[1]

            hist = historicData(ticker,
                                interval=timeframe,
                                startTime=f'{dateToMs(fromDate)}',
                                endTime=f'{dateToMs(toDate)}')

            db = db.append(hist)

        print('Listo')

        db.drop_duplicates(inplace=True)
        db.to_csv(os.getcwd() + '/csv/' + f'{ticker}-{timeframe}.csv')

        print(db)
    print("Descarga finalizada")

if __name__ == '__main__':
    main()
