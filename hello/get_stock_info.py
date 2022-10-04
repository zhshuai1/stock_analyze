import datetime
import sqlite3
import time
from urllib import request

AllIndustryBkUrl = 'http://22.push2.eastmoney.com/api/qt/clist/get?cb=jQuery112404207690664679433_0&pn=%(page_index)d&pz=%(page_size)d&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&wbp2u=|0|0|0|web&fid=f3&fs=m:90+t:2+f:!50&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f26,f22,f33,f11,f62,f128,f136,f115,f152,f124,f107,f104,f105,f140,f141,f207,f208,f209,f222'
AllConceptBkUrl = 'http://54.push2.eastmoney.com/api/qt/clist/get?cb=jQuery1124017979689482862038_0&pn=%(page_index)d&pz=%(page_size)d&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&wbp2u=|0|0|0|web&fid=f3&fs=m:90+t:3+f:!50&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f26,f22,f33,f11,f62,f128,f136,f115,f152,f124,f107,f104,f105,f140,f141,f207,f208,f209,f222'
BkInfoUrl = 'https://push2.eastmoney.com/api/qt/clist/get?cb=jQuery112307931529927219008_0&fid=f62&po=1&pz=%(page_size)d&pn=%(page_index)d&np=1&fltt=2&invt=2&ut=b2884a393a59ad64002292a3e90d46a5&fs=b:%(bk_name)s&fields=f12,f14,f2,f3,f62,f184,f66,f69,f72,f75,f78,f81,f84,f87,f204,f205,f124,f1,f13'
StockInfoUrl = 'http://push2.eastmoney.com/api/qt/stock/get?invt=2&fltt=1&cb=jQuery35104347076018161189_0&fields=f57,f107,f162,f152,f167,f92,f59,f183,f184,f105,f185,f186,f187,f173,f188,f84,f116,f85,f117,f190,f189,f62,f55&secid=%(type)s.%(code)s&ut=fa5fd1943c7b386f172d6893dbfba10b&wbp2u=|0|0|0|web'
HistoryUrl = 'http://32.push2his.eastmoney.com/api/qt/stock/kline/get?cb=jQuery35106362058314203287_0&secid=%(type)s.%(code)s&ut=fa5fd1943c7b386f172d6893dbfba10b&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61&klt=101&fqt=1&end=20500101&lmt=1000000'

to_json = lambda d: d
jQuery112404207690664679433_0 = to_json
jQuery1124017979689482862038_0 = to_json
jQuery112307931529927219008_0 = to_json
jQuery35104347076018161189_0 = to_json
jQuery35106362058314203287_0 = to_json
field_map = {
    'Close': 'f2',
    'Delta': 'f3',  # 涨幅
    'Exchange': 'f8',  # 换手率
    'Code': 'f12',
    'Type': 'f13',  # 0:sz; 1:sh; 90:bk
    'Name': 'f14',
    'High': 'f15',
    'Low': 'f16',
    'Open': 'f17',
    'CacheIn': 'f21',  # 流入资金
    'Volume': 'f47',  # 成交量
    'TurnOver': 'f48',  # 成交额
    'InnerVolume': 'f49',  # 场内成交量
    'Profit2': 'f55',  # 收益二
    'StockCode': 'f57',  # 编码
    'StockName': 'f58',  # 名称
    'YesterdayClose': 'f60',  # 昨收
    'TotalShare': 'f84',  # 总股本
    'CirculatingShare': 'f85',  # 流通股本
    'NetAssetPerShare': 'f92',  # 每股净资产
    'Profit': 'f105',  # 净利润
    'StockType': 'f110',  # 0:sz; 1:sh; 90:bk
    'MarketValue': 'f116',  # 总市值
    'CirculatingMarketValue': 'f117',  # 流通市值
    'IndustryBk': 'f127',  # 行业板块
    'AreaBk': 'f128',  # 地域板块
    'ConceptBk': 'f129',  # 概念板块
    'OuterVolume': 'f161',  # 场外成交量
    'DynamicPe': 'f162',  # PE(动)
    'Pb': 'f167',  # 市净率
    'Roe': 'f173',  # ROE
    'GrossProfitRatio': 'f186',  # 毛利率
    'NetProfitRatio': 'f187',  # 净利率
    'Revenue': 'f183',  # 营收
    'RevenueYoy': 'f184',  # 营收同比年增长
    'ProfitYoy': 'f185',  # 净利润同比年增长
    'DebtRatio': 'f188',  # 负债率
    'UndistributedProfitPerShare': 'f190',  # 每股未分配利润
    'IndustryBkCode': 'f198',  # 行业板块Code
}

rpc_count = 0
# db_name = '/Volumes/MySql/sqlite3/bk.db'
db_name = 'stock_info.db'


def get_all_industry_bk():
    res = []
    page_size = 20
    for page_index in range(1, 100):
        resp = request.urlopen(AllIndustryBkUrl % {'page_index': page_index, 'page_size': page_size})
        data = str(resp.read().decode()).strip(';')
        data = eval(data)['data']
        total = data['total']
        res.extend(data['diff'])
        # print('data len: %(len)d, data is: %(data)s' % {'len': len(data), 'data': data})
        if page_index * page_size >= total:
            break
    return res


def get_all_concept_bk():
    res = []
    page_size = 20
    for page_index in range(1, 100):
        resp = request.urlopen(AllConceptBkUrl % {'page_index': page_index, 'page_size': page_size})
        data = str(resp.read().decode()).strip(';')
        data = eval(data)['data']
        total = data['total']
        res.extend(data['diff'])
        # print('data len: %(len)d, data is: %(data)s' % {'len': len(data), 'data': data})
        if page_index * page_size >= total:
            break
    return res


def get_stocks_in_bk(bk_name):
    res = []
    page_size = 50
    for page_index in range(1, 100):
        resp = request.urlopen(
            BkInfoUrl % {'page_index': page_index, 'bk_name': bk_name, 'page_size': page_size})
        # 休息一下，防止问题
        time.sleep(0.3)
        # debug
        global rpc_count
        rpc_count += 1
        print('rpc called. count: %s' % rpc_count)
        data = str(resp.read().decode()).strip(';')
        # print('data len: %(len)d, data is: %(data)s' % {'len': len(data), 'data': data})
        data = eval(data)['data']
        if data is None:
            break
        total = data['total']
        res.extend(data['diff'])
        # print('data len: %(len)d, data is: %(data)s' % {'len': len(data), 'data': data})
        if page_index * page_size >= total:
            break
    return res


def get_all_bk_info():
    start = datetime.datetime.now()
    industry_bks = get_all_industry_bk()
    concept_bks = get_all_concept_bk()
    print('industry: \n%s\n concept: \n%s' % (len(industry_bks), len(concept_bks)))
    stocks = []
    total_bk = industry_bks + concept_bks
    for bk in total_bk:
        stock_in_this_bk = get_stocks_in_bk(bk[field_map['Code']])
        stocks.extend(stock_in_this_bk)
        print('bk_code: %s, bk_name:%s, stock size: %d' %
              (bk[field_map['Code']], bk[field_map['Name']], len(stocks)))
        print('time has cost: %s' % (datetime.datetime.now() - start).seconds)
        bk['stocks'] = stock_in_this_bk
    print('stocks: \n%s' % len(stocks))
    print('time cost: %s' % (datetime.datetime.now() - start).seconds)
    return industry_bks, concept_bks


def parse_daily_data(data):
    parts = data.split(',')
    date, open, close, high, low, volume, turnover, fluctuation, delta, diff, exchange_rate = parts
    return {'date': datetime.datetime.strptime(date, '%Y-%m-%d'),
            'open': float(open),
            'close': float(close),
            'high': float(high),
            'low': float(low),
            'volume': int(volume),
            'turnover': float(turnover),
            'fluctuation': float(fluctuation),
            'delta': float(delta),
            'diff': float(diff),
            'exchange_rate': float(exchange_rate),
            }


def get_all_stock_code():
    db = sqlite3.connect(db_name)
    cursor = db.cursor()
    cursor.execute('''select code, type, name from stock_info''')
    return cursor.fetchall()


def get_all_bk_code():
    db = sqlite3.connect(db_name)
    cursor = db.cursor()
    cursor.execute('''select code, type, name from bk_info''')
    return cursor.fetchall()


def get_history(type, code):
    res = []
    resp = request.urlopen(HistoryUrl % {'type': type, 'code': code})
    data = str(resp.read().decode()).strip(';')
    data = eval(data)['data']
    klines = data['klines']
    # print('data len: %(len)d, data is: %(data)s' % {'len': len(data), 'data': data})
    time.sleep(0.3)
    print('get history data for: %s.%s' % (type, code))
    return klines


def setup_db():
    db = sqlite3.connect(db_name)
    cursor = db.cursor()
    cursor.execute('''create table if not exists bk_info(
                        code varchar(10) primary key,
                        type varchar(10),
                        name varchar(20),
                        stocks text)
                        ''')
    cursor.execute('''create table if not exists stock_info(
                        code varchar(10) primary key,
                        type varchar(10),
                        name varchar(20),
                        industry_bk varchar(40),
                        concept_bks text)
                        ''')
    cursor.execute('''create table if not exists history(
                        code varchar(10) primary key,
                        type varchar(10),
                        name varchar(20),
                        history text)
                        ''')
