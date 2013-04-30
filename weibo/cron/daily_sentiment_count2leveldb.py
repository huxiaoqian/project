# -*- coding: utf-8 -*-

from xapian_weibo.xapian_backend import XapianSearch
import leveldb
import datetime
import time
import os

s = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline')

LEVELDBPATH = '/home/mirage/leveldb'
weibo_multi_sentiment_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'huyue_weibo_multi_sentiment'),
                                               block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
weibo_daily_sentiment_count_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'lijun_weibo_daily_sentiment_count'),
                                                     block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

emotions_kv = {'happy': 1, 'angry': 2, 'sad': 3}
total_days = 89

today = datetime.datetime.today()
now_ts = time.mktime(datetime.datetime(today.year, today.month, today.day, 2, 0).timetuple())
now_ts = int(now_ts)
during = 24 * 3600
begin_ts = now_ts - total_days * during

query_dict = {
    'timestamp': {'$gt': begin_ts, '$lt': now_ts},
}
count, get_results = s.search(query=query_dict, fields=['id', 'timestamp'])
print count

count = 0
ts = te = time.time()
daily_count_kv = {}
for r in get_results():
    count += 1
    if count % 10000 == 0:
        te = time.time()
        print count, '%s sec' % (te - ts)
        ts = te
    sentiment = weibo_multi_sentiment_bucket.Get(str(r['id']))
    if sentiment:
        sentiment = int(sentiment)
    if sentiment in emotions_kv.values():
        for i in xrange(-total_days + 1, 1):
            gt = now_ts + during * (i - 1)
            lt = now_ts + during * i
            if gt < r['timestamp'] <= lt:
                daily_count_kv.setdefault(str(lt) + '_' + str(sentiment), 0)
                daily_count_kv[str(lt) + '_' + str(sentiment)] += 1

print daily_count_kv
batch = leveldb.WriteBatch()
for k, v in daily_count_kv.iteritems():
    batch.Put(k, str(v))
weibo_daily_sentiment_count_bucket.Write(batch, sync=True)
