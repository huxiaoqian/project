# -*- coding: utf-8 -*-

import os
import time
import leveldb

from xapian_weibo.xapian_backend import XapianSearch

LEVELDBPATH = '/home/mirage/leveldb'

def datetime2ts(date):
    return time.mktime(time.strptime(date, '%Y-%m-%d'))

def make(hour):
    if hour < 10:
        db_name = 'hanyang_hourly_user_burst_20130307_0%s' % hour
    else:
        db_name = 'hanyang_hourly_user_burst_20130307_%s' % hour
        
    hourly_user_burst_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, db_name),
                                                    block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

    statuses_search = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_weibo', schema_version=2)

    user_search = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_user', schema_version=1)

    batch = leveldb.WriteBatch()

    start_ts = datetime2ts('2013-3-7') + (hour-1)*60*60
    end_ts = start_ts + hour*60*60

    query_dict = {'timestamp': {'$gt': start_ts, '$lt': end_ts}, 'reposts_count': {'$gt': 100}}

    statuses_count, get_statuses_results = statuses_search.search(query=query_dict, field=['user', '_id', 'reposts_count'])

    print 'total statuses: %s' % statuses_count

    batch.Put('size', str(statuses_count))

    count = 0
    uid_burst = {}
    for status in get_statuses_results():
        if count % 1000 == 0:
            print count
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

if __name__ == '__main__':
    for i in range(1, 25):
        make(i)
