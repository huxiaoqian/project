# -*- coding: utf-8 -*-

from xapian_weibo.xapian_backend import XapianSearch
import leveldb
import datetime
import time
import os
import redis

redis_host = 'localhost'
redis_port = 6379
redis_conn = redis.Redis(redis_host, redis_port)

s = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline')
LEVELDBPATH = '/home/mirage/leveldb'

user_daily_sentiment_count_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_user_daily_sentiment_count'),
                                                    block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
user_daily_field_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_user_daily_field'),
                                          block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
field_daily_sentiment_count_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_field_daily_sentiment_count'),
                                                    block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

emotions_kv = {'happy': 1, 'angry': 2, 'sad': 3}
fields_value = ['culture', 'education', 'entertainment', 'fashion', 'finance', 'media', 'sports', 'technology']

'''
以下方法针对xapian进行大规模list查询速度太慢，方法舍弃
def getUidsByFieldFromRedis(field, update_date='20130430', sorted_key='followers_count'):
    sorted_set_key = 'linhao_dailyfielduser_%s_%s_%s' % (update_date, field, sorted_key)
    start = 0
    end = redis_conn.zcard(sorted_set_key)-1
    results = redis_conn.zrange(sorted_set_key, start, end)
    uids = results
    return uids

def calculate_by_search_xapian(field):
    weibo_multi_sentiment_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'huyue_weibo_multi_sentiment'),
                                                   block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

    total_days = 89
    today = datetime.datetime.today()
    now_ts = time.mktime(datetime.datetime(today.year, today.month, today.day, 2, 0).timetuple())
    now_ts = int(now_ts)
    during = 24 * 3600
    begin_ts = now_ts - total_days * during

    uids = getUidsByFieldFromRedis(field)
    user_filter_list = [{'user': int(uid)} for uid in uids]

    query_dict_by_field = {
        'timestamp': {'$gt': begin_ts, '$lt': now_ts}, '$or': user_filter_list
    }
    count, get_results = s.search(query=query_dict_by_field, fields=['id', 'timestamp'])
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
                    daily_count_kv.setdefault(str(field) + '_' + str(lt) + '_' + str(sentiment), 0)
                    daily_count_kv[str(field) + '_' + str(lt) + '_' + str(sentiment)] += 1

    batch = leveldb.WriteBatch()
    for k, v in daily_count_kv.iteritems():
        batch.Put(k, str(v))
    field_daily_sentiment_count_bucket.Write(batch, sync=True)
'''

def main():
    count = 0
    ts = te = time.time()
    daily_count_kv = {}
    results = user_daily_sentiment_count_bucket.RangeIter()
    for k, v in results:
        count += 1
        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts)
            ts = te
        uid, lt, sentiment = k.split('_')
        try:
            u_field = user_daily_field_bucket.Get('%s_20130430' % uid).split(',')[0]
            try:
                sen_count = daily_count_kv[str(u_field) + '_' + str(lt) + '_' + str(sentiment)]
                daily_count_kv[str(u_field) + '_' + str(lt) + '_' + str(sentiment)] = str(int(sen_count) + int(v))
            except KeyError:
                daily_count_kv[str(u_field) + '_' + str(lt) + '_' + str(sentiment)] = str(v)
        except KeyError,e:
            continue
        
    print len(daily_count_kv.keys())

    batch = leveldb.WriteBatch()
    for k, v in daily_count_kv.iteritems():
        batch.Put(k, v)
    field_daily_sentiment_count_bucket.Write(batch, sync=True)

if __name__ == '__main__':
    main()
