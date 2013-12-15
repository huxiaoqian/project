# -*- coding: utf-8 -*-

import os
import time
import leveldb
import sys
from datetime import datetime

from xapian_weibo.xapian_backend import XapianSearch
from global_config import xapian_search_user as user_search, xapian_search_weibo as statuses_search, LEVELDBPATH

def datetime2ts(date):
    return time.mktime(time.strptime(date, '%Y-%m-%d'))

def ts2datetime(ts):
    return time.strftime('%Y-%m-%d', time.localtime(ts))

##LEVELDBPATH = '/home/mirage/leveldb'
##
##statuses_search = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_weibo', schema_version=2)
##
##user_search = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_user', schema_version=1)

def make(date):
    end_ts = datetime2ts(date)
    start_ts = end_ts - 24*60*60

    db_name = get_leveldb('active', end_ts)

    daily_user_active_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, db_name),
                                               block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

    batch = leveldb.WriteBatch()

    query_dict = {'timestamp': {'$gt': start_ts, '$lt': end_ts}}

    statuses_count, get_statuses_results = statuses_search.search(query=query_dict, field=['user', '_id', 'retweeted_status'])

    print 'total statuses: %s' % statuses_count

    print 'writing to levelDB %s...' % db_name

    count = 0

    uid_active = {}
    for status in get_statuses_results():
        if count % 10000 == 0:
            print 'current count: %s' % count
        try:
            repost = status['retweeted_status']
            isRepost = True
        except KeyError:
            isRepost = False
        uid = status['user']
        reposts_count = status['reposts_count']
        followers_count = 0
        if uid not in uid_active:
            uid_active[uid] = 0
        if isRepost:
            active = 0.5
        else:
            active = 0.5
        active += uid_active[uid]
        uid_active[uid] = active
        batch.Put(str(uid), str(active))
        count += 1

    daily_user_active_bucket.Write(batch, sync=True)

    print 'done.'

def read(date):
    end_ts = datetime2ts(date)
    
    db_name = get_leveldb('active', end_ts)

    daily_user_active_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, db_name),
                                               block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    count = 0
    for key, value in daily_user_active_bucket.RangeIter():
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
    current_time = datetime2ts('2013-9-30')

    date = ts2datetime(current_time)

    make(date)
    read(date)

if __name__ == '__main__': main()
    
