# -*- coding: utf-8 -*-

from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport
from thrift.protocol import TBinaryProtocol
from xapian_weibo.xapian_backend import XapianSearch
import json
import time
from datetime import datetime
import sys
sys.path.append('/home/hadoop/hbase-0.94.18/gen-py/')
from hbase import Hbase

host = 'localhost'
port = 9090
transport = TBufferedTransport(TSocket(host, port))
transport.open()
protocol = TBinaryProtocol.TBinaryProtocol(transport)

XAPIAN_FLUSH_DB_SIZE = 1000

client = Hbase.Client(protocol)

stub_file = '/media/data/stub/master_timeline_weibo_csv_20130928'
xapian_weibo = XapianSearch(stub=stub_file, schema_version=5)

def test_iter_all_docs():
    # 20 seconds per 10000 weibos
    weibos = xapian_weibo.iter_all_docs(fields=['_id'])
    count = 0
    tb = time.time()
    ts = tb    
    for weibo in weibos:
        mid = weibo['_id']
        bmiddle_pic = client.get('weibo', str(mid), 'o:timestamp', {})
        if bmiddle_pic != []:
            pass
            # print bmiddle_pic
        '''
        comments_count = client.get('weibo', str(mid), 'o', 'comments_count')
        geo =  client.get('weibo', str(mid), 'o', 'geo')
        reposts_count = client.get('weibo', str(mid), 'o', 'reposts_count')
        retweeted_mid = client.get('weibo', str(mid), 'o', 'retweeted_mid')
        retweeted_uid = client.get('weibo', str(mid), 'o', 'retweeted_uid')
        sentiment = client.get('weibo', str(mid), 'o', 'sentiment')
        terms = json.loads(client.get('weibo', str(mid), 'o', 'terms'))
        text = client.get('weibo', str(mid), 'o', 'text')
        timestamp = client.get('weibo', str(mid), 'o', 'timestamp')
        user = client.get('weibo', str(mid), 'o', 'user')
        '''

	if count % (XAPIAN_FLUSH_DB_SIZE * 10) == 0:
            te = time.time()
            print '[%s] search speed: %s sec/per %s' % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), te - ts, XAPIAN_FLUSH_DB_SIZE * 10)
            if count % (XAPIAN_FLUSH_DB_SIZE * 100) == 0:
                print '[%s] total search %s, cost: %s sec [avg: %sper/sec]' % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), count, te - tb, count / (te - tb))
            ts = te
        count += 1


def test_search_by_topic(topic):
    count, get_results = xapian_weibo.search(query={'text': topic}, fields=['_id'])
    print count
    count = 0
    tb = time.time()
    ts = tb 
    for r in get_results():
        mid = r['_id']
        bmiddle_pic = client.get('weibo', str(mid), 'o', {})
        if bmiddle_pic != []:
            pass

        if count % (XAPIAN_FLUSH_DB_SIZE * 10) == 0:
            te = time.time()
            print '[%s] search speed: %s sec/per %s' % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), te - ts, XAPIAN_FLUSH_DB_SIZE * 10)
            if count % (XAPIAN_FLUSH_DB_SIZE * 100) == 0:
                print '[%s] total search %s, cost: %s sec [avg: %sper/sec]' % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), count, te - tb, count / (te - tb))
            ts = te
        count += 1


if __name__ == '__main__':
    test_iter_all_docs()

    # test_search_by_topic(u'中国')
