# -*- coding: utf-8 -*-

import os
import sys
import time
import random
import operator
import leveldb

sys.path.append('../../identify/')

from user_sorter import user_rank

from datetime import datetime

LEVELDBPATH = '/home/mirage/leveldb'

def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print '%r %2.2f sec' % (method.__name__, te - ts)
        return result
    return timed

def datetime2ts(date):
    return time.mktime(time.strptime(date, '%Y-%m-%d'))

@timeit
def user_active_rank_test(date, window_size):
    date_time = datetime2ts(date)
    uid_active = {}
    if window_size == 1:
        db_name = get_leveldb('active', date_time)
        daily_user_active_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, db_name),
                                              block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
        
        for uid, active in daily_user_active_bucket.RangeIter():
            uid = int(uid)
            active = float(active)
            uid_active[uid] = active
    else:
        for i in range(window_size):
            db_name = get_leveldb('active', date_time - i*24*60*60)
            daily_user_active_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, db_name),
                                              block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
            for uid, active in daily_user_active_bucket.RangeIter():
                uid = int(uid)
                active = float(active)
                if uid not in uid_active:
                    uid_active[uid] = 0
                uid_active[uid] += active

    sorted_uids = user_rank(uid_active, 'active', 500, date, window_size)

    for uid in sorted_uids[:5]:
        print uid

@timeit
def user_important_rank_test(date, window_size):
    date_time = datetime2ts(date)
    uid_active = {}
    if window_size == 1:
        db_name = get_leveldb('important', date_time)
        daily_user_active_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, db_name),
                                              block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
        
        for uid, active in daily_user_active_bucket.RangeIter():
            uid = int(uid)
            active = float(active)
            uid_active[uid] = active
    else:
        for i in range(window_size):
            db_name = get_leveldb('active', date_time - i*24*60*60)
            daily_user_active_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, db_name),
                                              block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
            for uid, active in daily_user_active_bucket.RangeIter():
                uid = int(uid)
                active = float(active)
                if uid not in uid_active:
                    uid_active[uid] = 0
                uid_active[uid] += active

    sorted_uids = user_rank(uid_active, 'important', 500, date, window_size)

    for uid in sorted_uids[:5]:
        print uid

@timeit
def rank_simulation(uid_value):
    date = '2012-12-22'
    window_size = 1
    sorted_uids = user_rank(uid_value, 'test', 500, date, window_size)

@timeit
def raw_rank_simualtion(uid_value):
    sorted_uids = sorted(uid_value.iteritems(), key=operator.itemgetter(1))

def rank_data_simulation(size):
    uid_value = {}
    data = range(size)
    random.shuffle(data)
    for key, value in enumerate(data):
        uid_value[key] = value
    return uid_value

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
    '''
    用户数据
    '''
    # date = '2013-3-7'
    # window_size = 1
    # #time: 22.72s
    # user_active_rank_test(date, window_size)
    # #time: 20.87s
    # user_important_rank_test(date, window_size)

    '''
    模拟数据
    '''
    # #time: 28.49s
    # uid_value = rank_data_simulation(1000000)
    # rank_simulation(uid_value)
    # #time: 84.73s
    # uid_value = rank_data_simulation(10000000)
    # rank_simulation(uid_value)
    #time: 646.58s
    # uid_value = rank_data_simulation(100000000)
    # rank_simulation(uid_value)

    # #time: 1.89s
    # uid_value = rank_data_simulation(1000000)
    # raw_rank_simualtion(uid_value)
    # #time: 27.16s
    # uid_value = rank_data_simulation(10000000)
    # raw_rank_simualtion(uid_value)
    # #time: 572.47s
    # uid_value = rank_data_simulation(100000000)
    # raw_rank_simualtion(uid_value)

    '''
    性能测试结论
    数据层：
    1.基于Hadoop的排序需要将LevelDB中的键值对写入Hadoop的文件系统即HDFS中，对效率影响不大。
    2.Hadoop计算框架与其存储系统HDFS是密不可分的，如果能直接在HDFS上存储用户数据可以减少中间转化的操作。可以对各个模块的计算需求进行统一，建立统一的基于Hadoop的数据存储与计算模块。
    算法层：
    1.在用户数据上基于Hadoop的排序性能良好。
    2.在模拟数据上，对100万、1000万以及1亿个浮点数进行排序表明，对于少量数据，内存中排序优于基于Hadoop的排序。
      但随着数据增加，内存排序的优势越来越不明显。同时考虑到基于Hadoop的排序可以通过增加计算节点显著提高性能，故在真实数据宜采用Hadoop排序。
    '''

if __name__ == '__main__': main()
