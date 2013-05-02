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
user_daily_sentiment_count_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_user_daily_sentiment_count'),
                                                    block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

emotions_kv = {'happy': 1, 'angry': 2, 'sad': 3}
total_days = 89

today = datetime.datetime.today()
now_ts = time.mktime(datetime.datetime(today.year, today.month, today.day, 2, 0).timetuple())
now_ts = int(now_ts)
during = 24 * 3600
begin_ts = now_ts - total_days * during

query_dict = {
    'timestamp': {'$gt': begin_ts, '$lt': now_ts}
}
count, get_results = s.search(query=query_dict, fields=['user', 'id', 'timestamp'])
print count

'''
exist_uids = set()
count = 0
for r in get_results():
    if 'user' in r and r['user'] and 'id' in r['user']:
        exist_uids.add(r['user']['id']) 
        count += 1
print count

uid = exist_uids[0]

query_dict_by_uid = {
    'timestamp': {'$gt': begin_ts, '$lt': now_ts}, 'user': uid
}

count, get_results = s.search(query=query_dict_by_uid, fields=['id', 'timestamp'])
print count
'''

count = 0
ts = te = time.time()
daily_count_kv = {}
for r in get_results():
    if 'user' in r and r['user'] and 'id' in r['user']:
        uid = r['user']['id']
    else:
        count += 1
        continue
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
                daily_count_kv.setdefault(str(uid) + '_' + str(lt) + '_' + str(sentiment), 0)
                daily_count_kv[str(uid) + '_' + str(lt) + '_' + str(sentiment)] += 1

batch = leveldb.WriteBatch()
for k, v in daily_count_kv.iteritems():
    batch.Put(k, str(v))
user_daily_sentiment_count_bucket.Write(batch, sync=True)
