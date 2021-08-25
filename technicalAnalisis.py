"""
Herramientas para manejo de DataFrames con OHLC

@author: alexmnotfound
credits: gauss314
"""

def getHistoricoYFinance(symbol, start='2000-01-01', interval='1d', end=None):
    """
    Descarga de histórico de precios de Yahoo Finance

    :param symbol: Ticker a descargar
    :param start: Fecha de inicio (enero del 2000 por defecto)
    :param interval: Timeframe
    :param end: Fecha de fin
    :return:
    """
    import yfinance as yf

    data = yf.download(symbol, start=start, end=end, interval=interval, auto_adjust=True)

    return data


def getHistoricoBinance(symbol, interval='1d', startTime=None, endTime=None, limit=1000):
    '''
    Descarga de histórico de precios de Binance

    Minutos: 1m, 2m, 3m, 15m, 30m
    Horas: 1h, 2h, 4h
    Dias: 1d, 3d
    Meses: 1M
    '''
    import pandas as pd
    import requests

    # Defino URL y parámetros para request
    url = 'https://api.binance.com/api/v3/klines'

    params = {'symbol': symbol, 'interval': interval,
              'startTime': startTime, 'endTime': endTime, 'limit': limit}

    r = requests.get(url, params=params)
    js = r.json()

    # Armo el dataframe
    cols = ['openTime', 'Open', 'High', 'Low', 'Close', 'Volume', 'cTime',
            'qVolume', 'trades', 'takerBase', 'takerQuote', 'Ignore']

    df = pd.DataFrame(js, columns=cols)

    # Convierto los valores strings a numeros
    df = df.apply(pd.to_numeric)

    # Le mando indice de timestamp
    df.index = pd.to_datetime(df.openTime, unit='ms')

    # Elimino columnas que no quiero
    df = df.drop(['openTime', 'cTime', 'takerBase', 'takerQuote', 'Ignore'], axis=1)

    return df


def getDataExcel(ticker, timeframe):
    '''
    Busca excel de datos y devuelve DF
    '''
    import pandas as pd
    try:
        data = pd.read_excel(ticker + timeframe + '.xlsx').set_index('openTime').sort_index()
        #data.columns = ['Open', 'High', 'Low', 'Close', 'AdjClose', 'Volume']
        data['pctChange'] = data.Close.pct_change()
    except:
        try:
            data = pd.read_excel('excels_csvs/ADRs/' + ticker + '.xlsx').set_index('timestamp').sort_index()
            data.columns = ['Open', 'High', 'Low', 'Close', 'AdjClose', 'Volume']
            data['pctChange'] = data.AdjClose.pct_change()
        except:
            data = 'Sorry man no encontre el archivo en tus directorios'
    return data


def addPivots(df):
    '''
    Calculo los puntos pivote en base a un Dataframe.
    Los nombres de las columnas del DF deben estar en inglés (Open, High, Low, Close)

    '''
    df['PP'] = (df.High.shift(1) + df.Low.shift(1) + df.Close.shift(1)) / 3
    df['R1'] = (2 * df.PP) - df.Low.shift(1)
    df['R2'] = df.PP + (df.High.shift(1) - df.Low.shift(1))
    df['R3'] = df.High.shift(1) + (2 * (df.PP - df.Low.shift(1)))
    df['S1'] = (2 * df.PP) - df.High.shift(1)
    df['S2'] = df.PP - (df.High.shift(1) - df.Low.shift(1))
    df['S3'] = df.Low.shift(1) - (2 * (df.High.shift(1) - df.PP))

    '''
    df['PP'] = (df.High + df.Low + df.Close) / 3
    df['R1'] = (2 * df.PP) - df.Low
    df['R2'] = df.PP + (df.High - df.Low)
    df['R3'] = df.High + (2 * (df.PP - df.Low))
    df['S1'] = (2 * df.PP) - df.High
    df['S2'] = df.PP - (df.High - df.Low)
    df['S3'] = df.Low - (2 * (df.High - df.PP))
    '''
    return df


def addRSI(data, ruedas, ruedas_pend=0):
    """
    Agrega la columna RSI a nuestro dataframe, basado en su columna Close

    :param data: DataFrame con columna Close
    :param ruedas: integer, La cantidad de ruedas para el cálculo del RSI
    :param ruedas_pend: integer, opcional (Cantidad de ruedas para calcular pendiente del RSI y su divergencia)
    :return: DataFrame con RSI
    """
    import numpy as np
    df = data.copy()
    df['dif'] = df.Close.diff()
    df['win'] = np.where(df['dif'] > 0, df['dif'], 0)
    df['loss'] = np.where(df['dif'] < 0, abs(df['dif']), 0)
    df['ema_win'] = df.win.ewm(alpha=1 / ruedas).mean()
    df['ema_loss'] = df.loss.ewm(alpha=1 / ruedas).mean()
    df['rs'] = df.ema_win / df.ema_loss
    data['rsi'] = 100 - (100 / (1 + df.rs))

    if ruedas_pend != 0:
        data['rsi_pend'] = (data.rsi / data.rsi.shift(ruedas_pend) - 1) * 100
        precio_pend = (data.Close / data.Close.shift(ruedas_pend) - 1) * 100
        data['rsi_div'] = data.rsi_pend * precio_pend
    return data


def analizarDivergencias(data):
    divergencias_alcistas = data.loc[(data.rsi_div.shift() < 0) & (data.rsi_pend.shift() > 0) & (data.rsi.shift() < 35)]
    divergencias_bajistas = data.loc[(data.rsi_div.shift() < 0) & (data.rsi_pend.shift() < 0) & (data.rsi.shift() > 65)]
    div = {}
    div['alcista_media'] = divergencias_alcistas.pctChange.mean() * 100
    div['alcista_q'] = divergencias_alcistas.pctChange.count()
    div['bajista_media'] = divergencias_bajistas.pctChange.mean() * 100
    div['bajista_q'] = divergencias_bajistas.pctChange.count()
    div['q'] = div['alcista_q'] + div['bajista_q']
    div['sesgo'] = div['alcista_media'] - div['bajista_media']
    return div


def addMACD(data, slow=26, fast=12, suavizado=9):
    df = data.copy()
    df['ema_fast'] = df.Close.ewm(span=fast).mean()
    df['ema_slow'] = df.Close.ewm(span=slow).mean()
    data['macd'] = df.ema_fast - df.ema_slow
    data['signal'] = data.macd.ewm(span=suavizado).mean()
    data['histograma'] = data.macd - data.signal
    data = data.dropna().round(2)
    return data


def addBollinger(data, ruedas=20, desvios=2):
    data['sma_20'] = data.AdjClose.rolling(20).mean()
    volatilidad = data.AdjClose.rolling(20).std()
    data['boll_inf'] = data.sma_20 - 2 * volatilidad
    data['boll_sup'] = data.sma_20 + 2 * volatilidad
    data.dropna(inplace=True)
    return data


def addSMA(data, n):
    data['sma_' + str(n)] = data.Close.rolling(n).mean()
    return data


def addEMA(data, n):
    data['ema_' + str(n)] = data.Close.ewm(span=n).mean()
    return data


def addFW(data, n):
    data['fw_' + str(n)] = (data.Close.shift(-n) / data.AdjClose - 1) * 100
    return data

