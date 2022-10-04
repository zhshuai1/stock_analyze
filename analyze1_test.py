import unittest
import mysql.connector as connector
import sqlite3
import datetime
import pandas as pd
import numpy as np
import plotly.express as px


class Analyze1TestCase(unittest.TestCase):
    def test_gen_temp_table1(self):
        db = connector.connect(host='localhost',
                               user='zhangsh',
                               passwd='000000',
                               database='stock',
                               auth_plugin='mysql_native_password')
        start = datetime.datetime.now()
        cursor = db.cursor()
        cursor.execute('select distinct(code) from stock')
        res = cursor.fetchall()
        print(len(res))
        print(res)
        print("time cost %f" % (datetime.datetime.now() - start).seconds)

        # create tmp table
        disk_db = sqlite3.connect('disk_stock.db')
        disk_cursor = disk_db.cursor()
        disk_cursor.execute('''
        create table if not exists stock(
            id integer primary key autoincrement, 
            code varchar(10), 
            date datetime,
            open double,
            close double,
            high double,
            low double,
            authority double,
            delta double,
            volume bigint)
            ''')
        # disk_cursor.execute('''create unique index if not exists idx_code_date(code, date)''')

        for s in res:
            print("s is %s" % s)
            cursor.execute(
                "select code, date, open, close, high, low, authority, delta, volume from stock where code = %s",
                s)
            records = cursor.fetchall()
            for r in records:
                disk_cursor.execute('''insert into stock (code, date, open, close, high, low, authority, delta, volume) 
                values (?,?,?,?,?,?,?,?,?)''', (r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8]))
        disk_cursor.execute('commit')
        print('''dump time cost: %f''', (datetime.datetime.now() - start).seconds)
        self.assertTrue(True)

    def test_pandas_analysis(self):
        db = sqlite3.connect('disk_stock.db')
        db.row_factory = sqlite3.Row
        cursor = db.cursor()
        cursor.execute('''select * from stock''')
        records = cursor.fetchall()
        print("rows are %s" % records[:3])
        df = pd.DataFrame(data=records)
        df = df.rename(columns={0: 'id', 1: 'code', 2: 'date',
                                3: 'open', 4: 'close', 5: 'high',
                                6: 'low', 7: 'authority', 8: 'delta', 9: 'volume'})
        print('date type is: %s' % type(df['date'][0]))
        df.info()
        print(df.head())
        with_no_index_this_year = df.loc[(~df['code'].isin({'sh000001', 'sz399006', 'sz399001'})) &
                                         (df['date'] > '2022-01-01')]
        print("with_no_index_this_year: \n%s" % (with_no_index_this_year.head(),))
        grouped = with_no_index_this_year.groupby('code')
        close_data = grouped[['close']].agg([np.min, np.max])
        print("mmax: \n%s" % close_data.head())
        close_data['delta'] = close_data.loc[:, ['close', 'amax']].div(close_data.loc[:, ['close', 'amin']]) - 1
        close_data = close_data.sort_values(by='delta')
        date_data = grouped['date'].agg([np.min, np.max])

        print("mmax: \n%s" % close_data.head())
        print("mmin: \n%s" % date_data.head())
        ddf = pd.DataFrame(data={'code': with_no_index_this_year['code']})

        print()
        self.assertTrue(True)

    def test_pandas_analysis2(self):
        db = sqlite3.connect()
        self.assertTrue(True)

    def test_sample_data(self):
        start = datetime.datetime.now()
        src_db = sqlite3.connect('disk_stock.db')

        # src_db.row_factory = sqlite3.Row
        # src_cursor = src_db.cursor()
        # src_cursor.execute('''select * from stock where code not in (?) limit 9000''', ('sh000001',))
        # records = src_cursor.fetchall()

        records = pd.read_sql_query('''select * from stock''', src_db)
        # print('time cost to fetch data from db: %(time_cost)s' %
        #       {'time_cost': (datetime.datetime.now() - start).seconds})
        df = pd.DataFrame(data=records)
        # df.columns = list(map(lambda k: k, src_cursor.description))
        # print('data frame from db: \n%s' % df.head())
        df['type'] = df.apply(lambda s: s['code'][:2], axis=1)
        df = df.loc[(df['code'] == df['code'])
                    # & (df['date'] < '2022-06-01')
                    & (df['date'] > '2022-08-01')
                    & (~df['code'].isin(['sh000001', 'sz399001', 'sz399006']))
            # & (df['type'] == 'sz')
                    ]
        agg1 = df.groupby('code').agg(
            close_min=pd.NamedAgg(column='authority', aggfunc='min'),
            close_max=pd.NamedAgg(column='authority', aggfunc='max'),
            date_min=pd.NamedAgg(column='date', aggfunc='min'),
            date_max=pd.NamedAgg(column='date', aggfunc='max'),
        )
        # print('agg is: \n%s', agg1)

        stock2 = df.set_index(['code', 'date'])
        # print('stock2 is: \n%s', stock2)
        agg1['start'] = agg1.apply(lambda x: stock2.at[(x.name, agg1.at[x.name, 'date_min']), 'authority'], axis=1)
        agg1['end'] = agg1.apply(lambda x: stock2.at[(x.name, agg1.at[x.name, 'date_max']), 'authority'], axis=1)
        # print('stock2 is: \n%s', agg1)

        agg1['max_rise'] = agg1['close_max'] / agg1['close_min'] - 1
        agg1['total_rise'] = agg1['end'] / agg1['start'] - 1

        print(agg1.sort_values(by='total_rise', ascending=False).tail(30))

        hist = agg1.groupby(by=lambda r: np.ceil(agg1.at[r, 'total_rise'] / 0.01)).count()
        print('hist is: \n%s' % hist)
        fig = px.line(hist, x=hist.index, y='close_min')
        fig.show()
        print('hist is: \n%(hist)s' % {'hist': hist})
