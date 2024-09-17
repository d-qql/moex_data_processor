import urllib3
import xml.etree.ElementTree as ET
import pandas as pd
from io import StringIO
import datetime

requests = {
    'history_secs': 'http://iss.moex.com/iss/history/engines/%(engine)s/markets/%(market)s/boards/%(board)s/securities.json?date=%(date)s',
    'candles': 'http://iss.moex.com/iss/engines/%(engine)s/markets/%(market)s/boards/%(board)s/securities/%(security)s/candles.csv?from=%(from)s&till=%(till)s&interval=%(interval)s',
    'securities': 'https://iss.moex.com/iss/securities.csv?&engine=%(engine)s&market=%(market)s&start=%(start)s&limit=%(limit)s'
}

MOEX = ['AFKS', 'AFLT', 'AGRO', 'ALRS', 'BSPB', 'CBOM', 'CHMF', 'ENPG', 'FEES', 'FLOT', 'GAZP',
        'GLTR', 'GMKN', 'FIVE',
        'HYDR', 'IRAO', 'LEAS', 'LKOH', 'MAGN', 'MGNT', 'MOEX', 'MSNG', 'MTLR', 'MTLRP', 'MTSS', 'NLMK', 'NVTK',
        'OZON', 'PHOR', 'PIKK', 'PLZL', 'POSI', 'ROSN', 'RTKM', 'RUAL', 'SBER', 'SBERP', 'SELG', 'SGZH', 'SMLT', 'SNGS',
        'SNGSP', 'TATN', 'TATNP', 'TCSG', 'TRNFP', 'UPRO', 'VKCO', 'VTBR']


def to_io(string):
    strio = StringIO(string)
    strio.readline()
    return strio


def get_securities(pool, engine='stock', market='shares', start_line=0, limit=100):
    return pool.request('GET', requests['securities'] %
                        {'engine': engine,
                         'market': market,
                         'start': start_line,
                         'limit': limit}
                        ).data.decode('cp1251')


def get_engines(pool):
    return pool.request('GET', 'https://iss.moex.com/iss/engines.xml')


def get_candles(pool, security, date_from, date_till, interval):
    request = requests['candles'] % {'engine': 'stock',
                                     'market': 'shares',
                                     'board': 'TQBR',
                                     'security': security,
                                     'from': date_from,
                                     'till': date_till,
                                     'interval': interval}
    return pool.request('GET', request).data.decode('cp1251')


def to_df(response_str):
    sio = to_io(response_str)
    return pd.read_csv(sio, delimiter=';')


def all_securities(pool, rows, engine='stock', market='shares'):
    secs = []
    for start in range(0, rows, 100):
        limit = 100 if rows - start <= 100 else rows - start
        sec = to_df(get_securities(pool, engine, market, start, limit))
        secs = secs + sec['secid'].tolist()
    return secs


def volumes(pool, securities, date_from, date_till):
    dates = [date.date() for date in pd.date_range(date_from, date_till, freq='D')]
    data = {'Ticker': securities, **dict(zip([date.isoformat() for date in dates], [float('NaN')] * len(dates)))}
    df = pd.DataFrame(data)
    start_date = dates[0]
    for security in securities:
        while start_date <= dates[-1]:
            end_date = start_date + datetime.timedelta(days=100) if (start_date + datetime.timedelta(days=100)
                                                                     <= dates[-1]) else dates[-1]
            candle_df = to_df(get_candles(pool, security, start_date.isoformat(), end_date.isoformat(), 24))
            df.loc[df['Ticker'] == security, [value.split(' ')[0] for value in candle_df['begin'].values]] = \
                                                                                    candle_df['value'].values
            start_date = end_date + datetime.timedelta(days=1)
        start_date = dates[0]
        df = df.dropna(axis=1, how='all')
    return df

if __name__ == '__main__':
    http = urllib3.PoolManager()
    # candles = get_candles(http, 'SBER', '2024-09-01', '2024-09-13', '24')

    all = all_securities(http, 500)
    securities = list(filter(lambda x: len(x) < 6, all))

    df = volumes(http, MOEX, date_from='2024-01-01', date_till='2024-09-17')
    df.to_excel('volumes.xlsx')
