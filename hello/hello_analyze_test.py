import unittest
import sqlite3
from hello.get_stock_info import *
import pandas as pd
import numpy as np
import json
import plotly.express as px


def expanded_histories(code, type, name, histories):
    histories = json.loads(histories)
    res = []
    for h in histories:
        res.append(parse_daily_data(h))
    for r in res:
        r['code'], r['type'], r['name'] = code, type, name
    return res


class HelloAnalysisTestCase(unittest.TestCase):
    def test_load1(self):
        db = sqlite3.connect(db_name)
        df = pd.read_sql_query('''select * from history where type = 90''', db)
        df.info()
        # df = df.sample(n=10)
        print(df.head())
        histories = []
        for d in df.index:
            s = df.loc[d]
            histories.extend(expanded_histories(s['code'], s['type'], s['name'], s['history']))
        print('history shape: \n%s' % histories)
        df = pd.DataFrame(data=histories)
        industry_bks = get_all_industry_bk()
        industry_bk_codes = map(lambda bk: bk[field_map['Code']], industry_bks)
        df = df.loc[(df['date'] > '2022-05-01')
                    & (df.apply(lambda s: not s['name'].startswith('昨日'), axis=1))
                    & (df['code'].isin(industry_bk_codes))
                    ]
        print(df.head())
        dd = df.groupby('code', sort='date')['date'].idxmin()
        print('dd is: \n%s' % dd)
        df['delta'] = df['close'] / df.apply(lambda r: df.at[dd[r['code']], 'close'], axis=1)

        fig = px.line(df, x='date', y='delta', color='name')
        fig.show()
        self.assertTrue(True)
