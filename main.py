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


def extract_month(df, year, month):
    cols = [df.columns.tolist()[0]] + [strdate for strdate in df.columns.tolist()[1:] if
                                       datetime.date.fromisoformat(
                                           strdate).year == year and datetime.date.fromisoformat(
                                           strdate).month == month]
    month_df = df[cols]
    return month_df

def divide_months(df, start_date, end_date):
    monthes = [(date.year, date.month) for date in pd.date_range(start_date, end_date, freq='ME')]
    extracts = []
    if monthes[-1] != (end_date.year, end_date.month):
        monthes.append((end_date.year, end_date.month))
    for year, month in monthes:
        extracts.append((year, month, extract_month(df, year, month)))
    return extracts

def add_stats(df):
    with_stats = df.copy()
    with_stats['Средний объем'] = df.iloc[:, 1:].mean(axis=1)
    with_stats['Среднеквадратичное отклонение'] = df.iloc[:, 1:].std(axis=1)
    return with_stats

if __name__ == '__main__':
    http = urllib3.PoolManager()
    # candles = get_candles(http, 'SBER', '2024-09-01', '2024-09-13', '24')

    all = all_securities(http, 500)
    securities = list(filter(lambda x: len(x) < 6, all))

    start_date = datetime.date.fromisoformat('2023-01-01')
    end_date   = datetime.date.fromisoformat('2024-09-18')
    month_delta = (end_date.year - start_date.year)
    df = volumes(http, securities, date_from=start_date, date_till=end_date)
    divided_months = divide_months(df, start_date, end_date)
    df = add_stats(df)
    with pd.ExcelWriter('volumes_all.xlsx') as writer:
        df.to_excel(writer, sheet_name='Все дни')
        for year, month, month_df in divided_months:
            month_df = add_stats(month_df)
            month_df.to_excel(writer, sheet_name='{0:d}-{1:d}'.format(year, month))
