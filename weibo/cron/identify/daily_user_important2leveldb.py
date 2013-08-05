# -*- coding: utf-8 -*-

import os
import time
import leveldb

from datetime import datetime

from xapian_weibo.xapian_backend import XapianSearch

def datetime2ts(date):
    return time.mktime(time.strptime(date, '%Y-%m-%d'))

def ts2datetime(ts):
    return time.strftime('%Y-%m-%d', time.localtime(ts))

LEVELDBPATH = '/home/mirage/leveldb'

statuses_search = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_weibo', schema_version=2)

user_search = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_user', schema_version=1)

def make(date):
    end_ts = datetime2ts(date)
    start_ts = end_ts - 24*60*60

    db_name = get_leveldb('impotant', end_ts)

    daily_user_important_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, db_name),
                                                  block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

    batch = leveldb.WriteBatch()

    query_dict = {'timestamp': {'$gt': start_ts, '$lt': end_ts}, 'reposts_count': {'$gt': 500}}

    statuses_count, get_statuses_results = statuses_search.search(query=query_dict, field=['user', '_id', 'reposts_count'])

    print 'total statuses: %s' % statuses_count

    print 'writing to levelDB %s...' % db_name

    count = 0

    uid_important = {}
    for status in get_statuses_results():
        if count % 10000 == 0:
            print 'current count: %s' % count
        uid = status['user']
        reposts_count = status['reposts_count']
        followers_count = 0
        user_count, get_user_results = user_search.search(query={'_id': uid})
        if user_count == 1:
            for user in get_user_results():
                followers_count = user['followers_count']
        important = 0.9 * reposts_count + 0.1 * followers_count
        if uid not in uid_important:
            uid_important[uid] = 0
        important += uid_important[uid]
        uid_important[uid] = important
        batch.Put(str(uid), str(important))
        count += 1

    daily_user_important_bucket.Write(batch, sync=True)

    print 'done.'

def read(date):
    end_ts = datetime2ts(date)
    
    db_name = get_leveldb('important', end_ts)

    daily_user_important_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, db_name),
                                               block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    count = 0
    for key, value in daily_user_important_bucket.RangeIter():
        count += 1
    print 'total kvs: %s' % count

def get_leveldb(method, ts):
    date = datetime.fromtimestamp(ts)
    db_name = 'hanyang_daily_user_%s_' % method
    db_name += str(date.year)
    if date.month < 10:
        db_name += '0%s' % str(date.month)
    else:
        db_name += str(date.month)
    if date.day < 10:
        db_name += '0%s' % str(date.day)
    else:
        db_name += str(date.day)
    return db_name

def main():
    # current_time = time.time()
    current_time = datetime2ts('2013-3-1')

    date = ts2datetime(current_time)

    make(date)

if __name__ == '__main__': main()
