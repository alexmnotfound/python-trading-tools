"""
Armado de DataFrame y exportación a CSV de histórico de precios en Binance para distintos intervalos
@author: alexmnotfound
credits: gauss314
"""

import pandas as pd
import requests
import os
from datetime import datetime


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


def dateToMs(date, utc=(-3)):
    """
    Cambia fecha a MS
    :param date: str(AAAA-MM-DD)
    :param utc: time zone
    :return: ms
    """
    try:
        dt_obj = datetime.strptime(f'{date} 00:00:00',
                                   '%Y-%m-%d %H:%M:%S')
    except:
        try:
            dt_obj = datetime.strptime(f'{date}',
                                       '%Y-%m-%d %H:%M:%S')
        except Exception as e:
            print(f"No se pudo convertir la fecha: {e}")

    millisec = int(dt_obj.timestamp() * 1000) + (3600000*utc)

    return millisec


def main():

    tickers = ['1INCHUSDT', 'ETHUSDT']

    interval = '1h'

    fromDate = '2021-01-01'
    toDate = '2021-08-31'

    for ticker in tickers:

        # Creo DataFrame
        

        print(f'Descargando historial de {ticker}')

        hist = historicData(ticker,
                            interval=interval,
                            startTime=f'{dateToMs(fromDate)}',
                            endTime=f'{dateToMs(toDate)}')

        # Adjunto valores al DataFrame
        df = hist

        print(df)
        # Chequeo si el último row corresponde a la fecha final
        lastValue = dateToMs(hist.index[-1])

        while lastValue < dateToMs(toDate):
            hist = historicData(ticker,
                                interval=interval,
                                startTime=f'{lastValue}',
                                endTime=f'{dateToMs(toDate)}')

            lastValue = dateToMs(hist.index[-1])

            if lastValue == dateToMs(df.index[-1]):
                break

            df = df.append(hist)

            print(df)


        # Borro duplicados
        df.drop_duplicates(inplace=True)

        # Creo csv
        path = os.getcwd() + '\\csv\\test\\'
        fileName = f'{ticker}-{interval}.csv'
        df.to_csv(path + fileName)

        print(f"{fileName} creado en {path}")
    print("Programa finalizado")


if __name__ == '__main__':
    main()