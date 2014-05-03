# -*- coding: utf-8 -*-

import hurdles
import datetime
import time
import happybase
from hurdles.tools import extra_setup
from xapian_weibo.csv2json import itemLine2Dict
from xapian_weibo.xapian_backend import XapianSearch


class BenchHbaseR(hurdles.BenchCase):
    def setUp(self):
        thrift_server_host = '192.168.2.31'
        thrift_server_port = 9090
        hbase_table_name = 'weibo_test'
        connection = happybase.Connection(thrift_server_host, thrift_server_port)
        self.table = connection.table(hbase_table_name)
        # stub_file = '/media/data/stub/master_timeline_weibo_csv_20130928'
        # self.xapian_weibo = XapianSearch(stub=stub_file)
        self.weibos = self._load_items()

    def tearDown(self):
        pass

    def _load_items(self):
        weibos = []
        count = 0
        f = open('/media/data/original_data/csv/20130922_cut/1.csv')
        for line in f:
            weibo = itemLine2Dict(line)
            if weibo:
                weibos.append(weibo)
                count += 1
                if count == 10000:
                    break
        return weibos

    def bench_iter_all_docs(self):
        for weibo in self.weibos:
            mid = weibo['_id']
            row = self.table.row(str(mid))
