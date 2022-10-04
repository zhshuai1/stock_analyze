import json
import unittest

from hello.get_stock_info import *


class EastMoneyTestCase(unittest.TestCase):

    def test_strip(self):
        dd = "(abcd)".strip(')')
        print(dd)
        self.assertTrue(True)

    def test_convert(self):
        a = '-3.4'
        b = float(a)
        print('b type is: %s, and values is: %s, b+1.2: %s' % (type(b), b, b + 1.2))
        self.assertTrue(True)

    def test_get_url(self):
        all_bk = get_all_industry_bk()
        print('all_bk len: %d, data: %s' % (len(all_bk), all_bk))
        zhongyao_stocks = get_stocks_in_bk('BK1040')
        print('zhongyao_stocks len: %d, data: %s' % (len(zhongyao_stocks), zhongyao_stocks))
        self.assertTrue(True)

    def test_all_stocks(self):
        industry_bks, concept_bks = get_all_bk_info()
        # print('stocks: %s' % bks)
        bk_records = []
        stock_records = {}
        setup_db()
        for bk in industry_bks:
            bk_records.append({'code': bk[field_map['Code']],
                               'type': bk[field_map['Type']],
                               'name': bk[field_map['Name']],
                               'stocks': ','.join(list(map(lambda s: s[field_map['Code']], bk['stocks'])))})
            for s in bk['stocks']:
                stock_code = s[field_map['Code']]
                if stock_code not in stock_records:
                    stock_records[stock_code] = {'code': s[field_map['Code']],
                                                 'type': s[field_map['Type']],
                                                 'name': s[field_map['Name']],
                                                 'industry_bk': bk[field_map['Code']],
                                                 'concept_bks': ''}
                else:
                    stock_records[stock_code]['industry_bk'] += "," + bk[field_map['Code']]
        for bk in concept_bks:
            bk_records.append({'code': bk[field_map['Code']],
                               'type': bk[field_map['Type']],
                               'name': bk[field_map['Name']],
                               'stocks': ','.join(list(map(lambda s: s[field_map['Code']], bk['stocks'])))})
            for s in bk['stocks']:
                stock_code = s[field_map['Code']]
                if stock_code not in stock_records:
                    stock_records[stock_code] = {'code': s[field_map['Code']],
                                                 'type': s[field_map['Type']],
                                                 'name': s[field_map['Name']],
                                                 'industry_bk': '',
                                                 'concept_bks': bk[field_map['Code']]}
                else:
                    stock_records[stock_code]['concept_bks'] += "," + bk[field_map['Code']]
        stock_records = list(map(lambda p: p[1], stock_records.items()))
        for r in stock_records:
            r['concept_bks'] = r['concept_bks'].strip(',')
        db = sqlite3.connect(db_name)
        cursor = db.cursor()
        cursor.execute('delete from bk_info')
        cursor.execute('delete from stock_info')
        cursor.executemany('''insert into bk_info values(:code,:type,:name,:stocks)''', bk_records)
        cursor.executemany('''insert into stock_info values(:code,:type,:name,:industry_bk,:concept_bks)''',
                           stock_records)
        cursor.execute('commit')
        self.assertTrue(True)

    def test_all_stock_codes(self):
        codes = get_all_stock_code()
        print('all stock codes: %s' % codes)
        self.assertTrue(True)

    def test_get_history(self):
        max_history = 1000
        stocks = [] #get_all_stock_code()
        bks = get_all_bk_code()
        codes = stocks + bks
        db = sqlite3.connect(db_name)
        cursor = db.cursor()
        # cursor.execute('drop table history')
        setup_db()
        start = datetime.datetime.now()
        for c in codes:
            h = get_history(c[1], c[0])
            start_index = 0 if len(h) < max_history else len(h) - max_history
            cursor.execute('''insert into history values (?,?,?,?)''',
                           (c[0], c[1], c[2], json.dumps(h[start_index:])))
            cursor.execute('commit')
            print('time cost: %s' % (datetime.datetime.now() - start).seconds)
        self.assertTrue(True)

    def test_setup_db(self):
        setup_db()
        self.assertTrue(True)

    def test_gen_fields(self):
        s = ",".join(['f' + str(i) for i in range(1, 250)])
        print(s)
        self.assertTrue(True)
