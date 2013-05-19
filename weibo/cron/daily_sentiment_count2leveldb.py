# -*- coding: utf-8 -*-

from xapian_weibo.xapian_backend_extra import Schema
from xapian_weibo.xapian_backend import XapianSearch
import leveldb
import datetime
import time
import os

LEVELDBPATH = '/home/mirage/leveldb'
weibo_daily_sentiment_count_global_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'lijun_weibo_daily_sentiment_count_global'),
                                                            block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

total_days = 90
emotions_kv = {'happy': 1, 'angry': 2, 'sad': 3}
s = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_sentiment', schema=Schema, schema_version=1)

today = datetime.datetime.today()
now_ts = time.mktime(datetime.datetime(today.year, today.month, today.day, 2, 0).timetuple())
now_ts = int(now_ts)
during = 24 * 3600

for i in xrange(-total_days + 1, 1):
    begin_ts = now_ts + during * (i - 1)
    end_ts = now_ts + during * i
    print i, begin_ts, end_ts
    for emotion in emotions_kv.keys():
        query_dict = {
            'timestamp': {'$gt': begin_ts, '$lt': end_ts},
            'sentiment': emotions_kv[emotion],
        }
        count, _ = s.search(query=query_dict)
        weibo_daily_sentiment_count_global_bucket.Put(str(end_ts) + '_' + str(emotions_kv[emotion]), str(count))
