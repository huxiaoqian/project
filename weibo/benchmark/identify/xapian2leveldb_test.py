# -*- coding: utf-8 -*-

import sys
import time

import profile

from xapian_weibo.xapian_backend import XapianSearch

XAPIAN_STATUSES_PATH  ='/opt/xapian_weibo/data/faked/'
XAPIAN_USER_PATH  ='/opt/xapian_weibo/data/'

sys.path.append('../../cron/identify')

def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print '%r args: %s %2.2f sec' % (method.__name__, args, te - ts)
        return result
    return timed

def datetime2ts(date):
    return time.mktime(time.strptime(date, '%Y-%m-%d'))

@timeit
def search_test(date):
    start_ts = datetime2ts(date)
    end_ts = start_ts + 24*60*60
    statuses_search = XapianSearch(path=XAPIAN_STATUSES_PATH, name='master_timeline_weibo', schema_version=2)
    query_dict = {'timestamp': {'$gt': start_ts, '$lt': end_ts}}
    statuses_count, get_statuses_results = statuses_search.search(query=query_dict, field=['user', '_id', 'retweeted_status'])
    count = 0
    start_time = time.time()
    for status in get_statuses_results():
        count += 1
    print 'total statuses %s' % count

#daily_user_active
@timeit
def daily_user_active_write_test(date):
    from daily_user_active2leveldb import make
    make(date)

@timeit
def daily_user_active_read_test(date):
    from daily_user_active2leveldb import read
    read(date)

#daily_user_important
@timeit
def daily_user_important_write_test(date):
    from daily_user_import2leveldb import make
    make(date)

@timeit
def daily_user_important_read_test(date):
    from daily_user_import2leveldb import read
    read(date)

#hourly_user_burst
@timeit
def hourly_user_burst_write_test(date):
    from hourly_user_burst2leveldb import make
    hour = 12
    make(date, hour)

@timeit
def hourly_user_burst_read_test(date):
    from hourly_user_burst2leveldb import read
    hour = 12
    read(date, hour)

def main():
    date = '2013-3-1'

    # #time: 1243.53s
    # search_test(date)

    # #time: 483.05s
    # daily_user_active_write_test(date)
    # #time: 0.77s
    # daily_user_active_read_test(date)

    # #time: 42.75s
    # daily_user_important_write_test(date)
    # #time: 0.82s
    # daily_user_important_read_test(date)

    # #time: 109.73s
    # hourly_user_burst_write_test(date)
    # #time: 0.15s
    # hourly_user_burst_read_test(date)


    '''
    性能测试结论
    数据层：
    1.从搜索引擎根据时间检索微博的效率还有待提高，对于模拟数据检索一天的微博需要接近20分钟。考虑到实际环境每天2亿的微博数据，在更大规模的数据集上后台数据转化任务的压力会加大。
    2.从微博数据统计用户行为数据，如发微博、被转发等，并写入LevelDB的性能良好，读取LevelDB的性能优异。LevelDB在实测中可以达到写入50MB/秒以及读取200MB/秒。
    3.鉴于从微博索引层提取数据的效率不高，现有方式可以改为从推送的数据流中提取信息直接存入内存数据库，动态维护实时的用户统计数据，并定期将数据转移至外存。
    4.用户粉丝数据、评论数据在测试索引层中不存在，故无法进行测试。
    '''

if __name__ == '__main__': main()
