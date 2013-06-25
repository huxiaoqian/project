# -*- coding: utf-8 -*-

import os
import time
import leveldb

from xapian_weibo.xapian_backend import XapianSearch

def datetime2ts(date):
    return time.mktime(time.strptime(date, '%Y-%m-%d'))

LEVELDBPATH = '/home/mirage/leveldb'
daily_user_important_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'hanyang_daily_user_important_20130307'),
                                                    block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

statuses_search = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_weibo', schema_version=2)

user_search = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_user', schema_version=1)

batch = leveldb.WriteBatch()

def make():
    start_ts = datetime2ts('2013-3-7')
    end_ts = datetime2ts('2013-3-8')

    query_dict = {'timestamp': {'$gt': start_ts, '$lt': end_ts}, 'reposts_count': {'$gt': 500}}

    statuses_count, get_statuses_results = statuses_search.search(query=query_dict, field=['user', '_id', 'reposts_count'])

    print 'total statuses: %s' % statuses_count

    count = 0

    uid_important = {}
    for status in get_statuses_results():
        if count % 1000 == 0:
            print count
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

def read():
    for key, value in daily_user_important_bucket.RangeIter():
        print key, value

if __name__ == '__main__': make()
