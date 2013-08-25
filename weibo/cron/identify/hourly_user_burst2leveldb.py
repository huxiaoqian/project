# -*- coding: utf-8 -*-

import os
import time
import leveldb

from datetime import datetime

from xapian_weibo.xapian_backend import XapianSearch

LEVELDBPATH = '/home/mirage/leveldb'

def datetime2ts(date):
    return time.mktime(time.strptime(date, '%Y-%m-%d'))

def ts2datetime(ts):
    return time.strftime('%Y-%m-%d', time.localtime(ts))

def make(date, hour):
    ts = datetime2ts(date)
    start_ts = datetime2ts(date) + (hour-1)*60*60
    end_ts = start_ts + hour*60*60

    db_name = get_leveldb(ts, hour)
        
    hourly_user_burst_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, db_name),
                                                    block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

    statuses_search = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_weibo', schema_version=2)

    user_search = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_user', schema_version=1)

    batch = leveldb.WriteBatch()

    query_dict = {'timestamp': {'$gt': start_ts, '$lt': end_ts}, 'reposts_count': {'$gt': 100}}

    statuses_count, get_statuses_results = statuses_search.search(query=query_dict, field=['user', '_id', 'reposts_count'])

    print 'total statuses: %s' % statuses_count

    print 'writing to levelDB %s...' % db_name

    batch.Put('size', str(statuses_count))

    count = 0
    uid_burst = {}
    for status in get_statuses_results():
        if count % 10000 == 0:
            print 'current count: %s' % count
        uid = status['user']
        reposts_count = status['reposts_count']
        followers_count = 0
        if uid not in uid_burst:
            uid_burst[uid] = 0
        reposts_count += uid_burst[uid]
        uid_burst[uid] = reposts_count
        batch.Put(str(uid), str(reposts_count))
        count += 1

    hourly_user_burst_bucket.Write(batch, sync=True)

    print 'done.'

def read(date, hour):
    end_ts = datetime2ts(date)

    db_name = get_leveldb(end_ts, hour)

    hourly_user_burst_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, db_name),
                                               block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    count = 0
    for key, value in hourly_user_burst_bucket.RangeIter():
        count += 1
    print 'total kvs: %s' % count

def get_leveldb(ts, hour):
    date = datetime.fromtimestamp(ts)
    db_name = 'hanyang_hourly_user_burst_'
    db_name += str(date.year)
    if date.month < 10:
        db_name += '0%s' % str(date.month)
    else:
        db_name += str(date.month)
    if date.day < 10:
        db_name += '0%s' % str(date.day)
    else:
        db_name += str(date.day)
    if hour < 10:
        db_name += '_0%s' % hour
    else:
        db_name += '_%s' % hour
    return db_name

def main():
    # current_time = time.time()
    current_time = datetime2ts('2013-3-1') + 12*60*60

    current_datetime = datetime.fromtimestamp(current_time)
    current_hour = current_datetime.hour

    date = ts2datetime(current_time)

    make(date, current_hour)

if __name__ == '__main__': main()
