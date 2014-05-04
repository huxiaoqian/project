# -*- coding: utf-8 -*-

import json
import time
from datetime import datetime
import happybase
from xapian_weibo.xapian_backend import XapianSearch

XAPIAN_FLUSH_DB_SIZE = 1000

stub_file = '/home/ubuntu12/dev/data/stub/master_timeline_weibo_20130921'
xapian_weibo = XapianSearch(stub=stub_file, schema_version=5)

def test_iter_all_docs():
    # 20 seconds per 10000 weibos
    weibos = xapian_weibo.iter_all_docs(fields=['_id', 'bmiddle_pic', 'comments_count', 'geo', \
                                                'reposts_count', 'retweeted_mid', 'retweeted_uid', \
                                                'sentiment', 'terms', 'text', 'timestamp', 'user'])
    count = 0
    tb = time.time()
    ts = tb    
    for weibo in weibos:
        row = weibo
    	if row != {}:
            bmiddle_pic = row['bmiddle_pic']
            comments_count = row['comments_count']
            geo = row['geo']
            reposts_count = row['reposts_count']
            retweeted_mid = row['retweeted_mid']
            retweeted_uid = row['retweeted_uid']
            sentiment = row['sentiment']
            terms = row['terms']
            text = row['text']
            timestamp = row['timestamp']
            user = row['user']

    	if count % (XAPIAN_FLUSH_DB_SIZE * 10) == 0:
            te = time.time()
            print '[%s] search speed: %s sec/per %s' % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), te - ts, XAPIAN_FLUSH_DB_SIZE * 10)
            if count % (XAPIAN_FLUSH_DB_SIZE * 100) == 0:
                print '[%s] total search %s, cost: %s sec [avg: %sper/sec]' % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), count, te - tb, count / (te - tb))
            ts = te
        count += 1


def test_search_by_topic(topic):
    count, get_results = xapian_weibo.search(query={'text': topic}, fields=['_id', 'bmiddle_pic', 'comments_count', 'geo', \
                                                'reposts_count', 'retweeted_mid', 'retweeted_uid', \
                                                'sentiment', 'terms', 'text', 'timestamp', 'user'])
    print count
    count = 0
    tb = time.time()
    ts = tb 
    for r in get_results():
        row = r
        if row != {}:
            bmiddle_pic = row['bmiddle_pic']
            comments_count = row['comments_count']
            geo = row['geo']
            reposts_count = row['reposts_count']
            retweeted_mid = row['retweeted_mid']
            retweeted_uid = row['retweeted_uid']
            sentiment = row['sentiment']
            terms = row['terms']
            text = row['text']
            timestamp = row['timestamp']
            user = row['user']

        if count % (XAPIAN_FLUSH_DB_SIZE * 10) == 0:
            te = time.time()
            print '[%s] search speed: %s sec/per %s' % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), te - ts, XAPIAN_FLUSH_DB_SIZE * 10)
            if count % (XAPIAN_FLUSH_DB_SIZE * 100) == 0:
                print '[%s] total search %s, cost: %s sec [avg: %sper/sec]' % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), count, te - tb, count / (te - tb))
            ts = te
        count += 1


if __name__ == '__main__':
    # test_iter_all_docs()

    test_search_by_topic(u'中国')
