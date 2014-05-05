# -*- coding: utf-8 -*-

import json
import time
from datetime import datetime
import happybase
from xapian_weibo.xapian_backend import XapianSearch

thrift_server_host = '192.168.2.31'
thrift_server_port = 9090
hbase_table_name = 'weibo'
XAPIAN_FLUSH_DB_SIZE = 1000
connection = happybase.Connection(thrift_server_host, thrift_server_port)
table = connection.table(hbase_table_name)

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
        row = table.row(str(mid))
    	if row != {}:
            bmiddle_pic = row['o:bmiddle_pic']
            comments_count = row['o:comments_count']
            geo = row['o:geo']
            reposts_count = row['o:reposts_count']
            retweeted_mid = row['o:retweeted_mid']
            retweeted_uid = row['o:retweeted_uid']
            sentiment = row['o:sentiment']
            terms = json.loads(row['o:terms'])
            text = row['o:text']
            timestamp = row['o:timestamp']
            user = row['o:user']

    	if count % (XAPIAN_FLUSH_DB_SIZE * 10) == 0:
            te = time.time()
            print '[%s] search speed: %s sec/per %s' % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), te - ts, XAPIAN_FLUSH_DB_SIZE * 10)
            if count % (XAPIAN_FLUSH_DB_SIZE * 100) == 0:
                print '[%s] total search %s, cost: %s sec [avg: %sper/sec]' % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), count, te - tb, count / (te - tb))
            ts = te
        count += 1


def test_iter_all_docs_columns():
    # 10 seconds per 10000 weibos
    weibos = xapian_weibo.iter_all_docs(fields=['_id'])
    count = 0
    tb = time.time()
    ts = tb    
    for weibo in weibos:
        mid = weibo['_id']
        row = table.row(str(mid), columns=['o:bmiddle_pic', 'o:comments_count', 'o:geo', 'o:reposts_count', \
                                           'o:retweeted_mid', 'o:retweeted_uid', 'o:sentiment', 'o:terms', \
                                           'o:text', 'o:timestamp', 'o:user'])
        if row != {}:
            bmiddle_pic = row['o:bmiddle_pic']
            comments_count = row['o:comments_count']
            geo = row['o:geo']
            reposts_count = row['o:reposts_count']
            retweeted_mid = row['o:retweeted_mid']
            retweeted_uid = row['o:retweeted_uid']
            sentiment = row['o:sentiment']
            terms = json.loads(row['o:terms'])
            text = row['o:text']
            timestamp = row['o:timestamp']
            user = row['o:user']

        if count % (XAPIAN_FLUSH_DB_SIZE * 10) == 0:
            te = time.time()
            print '[%s] search speed: %s sec/per %s' % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), te - ts, XAPIAN_FLUSH_DB_SIZE * 10)
            if count % (XAPIAN_FLUSH_DB_SIZE * 100) == 0:
                print '[%s] total search %s, cost: %s sec [avg: %sper/sec]' % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), count, te - tb, count / (te - tb))
            ts = te
        count += 1


def test_iter_all_docs_batch(batch_size=10000):
    # 20 seconds per 10000 weibos
    weibos = xapian_weibo.iter_all_docs(fields=['_id'])
    count = 0
    tb = time.time()
    ts = tb
    batch_mids = []
    for weibo in weibos:
        mid = weibo['_id']
        batch_mids.append(str(mid))

        if count % 10000 == 0:
            rows = table.rows(batch_mids)
            for key, row in rows:
                if row != {}:
                    bmiddle_pic = row['o:bmiddle_pic']
                    comments_count = row['o:comments_count']
                    geo = row['o:geo']
                    reposts_count = row['o:reposts_count']
                    retweeted_mid = row['o:retweeted_mid']
                    retweeted_uid = row['o:retweeted_uid']
                    sentiment = row['o:sentiment']
                    terms = json.loads(row['o:terms'])
                    text = row['o:text']
                    timestamp = row['o:timestamp']
                    user = row['o:user']
            batch_mids = []

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
        row = table.row(str(mid))
        if row != {}:
            bmiddle_pic = row['o:bmiddle_pic']
            comments_count = row['o:comments_count']
            geo = row['o:geo']
            reposts_count = row['o:reposts_count']
            retweeted_mid = row['o:retweeted_mid']
            retweeted_uid = row['o:retweeted_uid']
            sentiment = row['o:sentiment']
            terms = json.loads(row['o:terms'])
            text = row['o:text']
            timestamp = row['o:timestamp']
            user = row['o:user']

        if count % (XAPIAN_FLUSH_DB_SIZE * 10) == 0:
            te = time.time()
            print '[%s] search speed: %s sec/per %s' % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), te - ts, XAPIAN_FLUSH_DB_SIZE * 10)
            if count % (XAPIAN_FLUSH_DB_SIZE * 100) == 0:
                print '[%s] total search %s, cost: %s sec [avg: %sper/sec]' % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), count, te - tb, count / (te - tb))
            ts = te
        count += 1


def test_search_by_topic_batch(topic, batch_size=10000):
    count, get_results = xapian_weibo.search(query={'text': topic}, fields=['_id'])
    print count
    count = 0
    tb = time.time()
    ts = tb
    batch_mids = []
    for r in get_results():
        mid = r['_id']
        batch_mids.append(str(mid))

        if count % 10000 == 0:
            rows = table.rows(batch_mids)
            for key, row in rows:
                if row != {}:
                    bmiddle_pic = row['o:bmiddle_pic']
                    comments_count = row['o:comments_count']
                    geo = row['o:geo']
                    reposts_count = row['o:reposts_count']
                    retweeted_mid = row['o:retweeted_mid']
                    retweeted_uid = row['o:retweeted_uid']
                    sentiment = row['o:sentiment']
                    terms = json.loads(row['o:terms'])
                    text = row['o:text']
                    timestamp = row['o:timestamp']
                    user = row['o:user']
            batch_mids = []

        if count % (XAPIAN_FLUSH_DB_SIZE * 10) == 0:
            te = time.time()
            print '[%s] search speed: %s sec/per %s' % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), te - ts, XAPIAN_FLUSH_DB_SIZE * 10)
            if count % (XAPIAN_FLUSH_DB_SIZE * 100) == 0:
                print '[%s] total search %s, cost: %s sec [avg: %sper/sec]' % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), count, te - tb, count / (te - tb))
            ts = te
        count += 1


if __name__ == '__main__':
    # test_iter_all_docs()

    # test_search_by_topic(u'中国')

    # test_iter_all_docs_columns()

    # test_iter_all_docs_batch()
    
    # 40 seconds per 10000 weibos
    test_search_by_topic_batch(u'中国')
    
    # seconds per 10000 weibos
    test_search_by_topic_batch(u'中国', 5000)
