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

MOEX = ['IMOEX', 'AFKS', 'AFLT', 'AGRO', 'ALRS', 'SBPB', 'CBOM', 'CHMF', 'ENPG', 'FEES', 'FIVE', 'FLOT', ' GAZP', 'GLTR', 'GMKN',
        'HYDR', 'IRAO', 'LEAS', 'LKOH', 'MAGN', ' MGNT', 'MOEX', 'MSNG', 'MTLR', 'MTLRP', 'MTSS', 'NLMK', 'NVTK',
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
    print(secs)
    return secs


def volumes(pool, securities, date_from, date_till):
    data = {'Тикер': securities}
    df = pd.DataFrame(data)
    df = df.set_index('Тикер')
    start_date = datetime.date.fromisoformat(date_from)
    end_date = datetime.date.fromisoformat(date_till)
    while start_date <= end_date:
        print(start_date.isoformat())
        df[start_date.isoformat()] = ''
        for sec in securities:
            candle_df = to_df(
                get_candles(pool, security=sec, date_from=start_date.isoformat(),
                            date_till=start_date.isoformat(),
                            interval=24))
            vals = candle_df['value'].values
            if len(vals) != 0:
                df.at[sec, start_date.isoformat()] = vals[0]
        start_date = start_date + datetime.timedelta(days=1)
    return df


if __name__ == '__main__':
    http = urllib3.PoolManager()
    # candles = get_candles(http, 'SBER', '2024-09-01', '2024-09-13', '24')

    all = all_securities(http, 500)
    securities = list(filter(lambda x: len(x) < 6, all))

    df = volumes(http, MOEX, date_from='2024-01-01', date_till='2024-09-15')
    df.to_excel('volumes.xlsx')
