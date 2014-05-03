# -*- coding: utf-8 -*-

import time
import json
import hurdles
import datetime
import happybase
from hurdles.tools import extra_setup
from xapian_weibo.utils import load_scws, cut
from xapian_weibo.csv2json import itemLine2Dict
from classifier.triple_sentiment_classifier import triple_classifier

XAPIAN_EXTRA_FIELD = 'sentiment'

s = load_scws()

def fill_sentiment(item):
    sentiment = triple_classifier(item)
    item[XAPIAN_EXTRA_FIELD] = sentiment
    return item

def cut_text(item):
    text = item['text'].encode('utf-8')
    item['terms'] = cut(s, text, cx=False)
    return item

def _utf_8_encode(stri):
    try:
        stri = str(stri)
    except:
        stri = str(stri.encode('utf-8'))
    return stri

class BenchHbaseW(hurdles.BenchCase):
    def setUp(self):
        thrift_server_host = '192.168.2.31'
        thrift_server_port = 9090
        hbase_table_name = 'weibo_test'
        self.columnFamily = 'o'
        connection = happybase.Connection(thrift_server_host, thrift_server_port)
        self.table = connection.table(hbase_table_name)
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

    def _write_hbase_item(self, item):
        row_key = str(item['_id'])
        value_dict = {}
        for k, v in item.iteritems():
            column = '%s:%s' % (self.columnFamily, k)
            if k == 'terms':
                value = json.dumps(v)
            else:
                value = _utf_8_encode(v)
            value_dict[column] = value

        self.table.put(row_key, value_dict)

    def bench_single_write(self):
        for weibo in self.weibos:
            weibo = fill_sentiment(weibo)
            weibo = cut_text(weibo)
            self._write_hbase_item(weibo)
