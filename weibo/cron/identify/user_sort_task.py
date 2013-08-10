# -*- coding: utf-8 -*-

import os
import math
import operator
import leveldb

from xapian_weibo.xapian_backend import XapianSearch

from user_sorter import user_rank
from utils import get_leveldb, save_rank_results
from time_utils import datetime2ts, ts2datetime, window2time

from config import FOLLOWERS_MIN_SUPPORT, REPOSTS_MIN_SUPPORT, BURST_MIN_SUPPORT

LEVELDBPATH = '/home/mirage/leveldb'

def save_to_db(method):
    def func(*args, **kw):
        data = method(*args, **kw)
        method_name = method.__name__
        rank_range = method_name.split('_')[0]
        rank_field = method_name.split('_')[1]
        save_rank_results(data, rank_range, rank_field, args[1], args[2])
        return data
    return func

@save_to_db
def whole_followers_task(top_n, date, window_size):
    user_search = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_user', schema_version=1)
    count, get_results = user_search.search(query={'followers_count': {'$gt': FOLLOWERS_MIN_SUPPORT}}, sort_by=['-followers_count'], fields=['_id'], max_offset=top_n)
    sorted_uids = []
    for user in get_results():
        sorted_uids.append(user['_id'])
    return sorted_uids

@save_to_db
def whole_active_task(top_n, date, window_size):
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

    if len(uid_active) < 100000000:
        sorted_uid_active = sorted(uid_active.iteritems(), key=operator.itemgetter(1), reverse=True)
        sorted_uids = []
        count = 0
        for uid, value in sorted_uid_active:
            if count >= top_n:
                break
            sorted_uids.append(uid)
        count += 1
    else:
        sorted_uids = user_rank(uid_active, 'whole_active', top_n, date, window_size)

    return sorted_uids

@save_to_db
def whole_important_task(top_n, date, window_size):
    date_time = datetime2ts(date)
    uid_important = {}
    if window_size == 1:
        db_name = get_leveldb('important', date_time)
        daily_user_important_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, db_name),
                                              block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
        for uid, important in daily_user_important_bucket.RangeIter():
            uid = int(uid)
            important = float(important)
            uid_important[uid] = important
    else:
        for i in range(window_size):
            db_name = get_leveldb('important', date_time - i*24*60*60)
            daily_user_important_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, db_name),
                                              block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
            for uid, important in daily_user_important_bucket.RangeIter():
                uid = int(uid)
                important = float(important)
                if uid not in uid_important:
                    uid_important[uid] = 0
                uid_important[uid] += important

    if len(uid_important) < 100000000:
        sorted_uid_important = sorted(uid_important.iteritems(), key=operator.itemgetter(1), reverse=True)
        sorted_uids = []
        count = 0
        for uid, value in sorted_uid_important:
            if count >= top_n:
                break
            sorted_uids.append(uid)
        count += 1
    else:
        sorted_uids = user_rank(uid_important, 'whole_active', top_n, date, window_size)

    return sorted_uids

@save_to_db
def burst_followers_task(top_n, date, window_size):
    user_search = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_user', schema_version=1)
    count, get_results = user_search.search(query={'followers_count': {'$gt': FOLLOWERS_MIN_SUPPORT}}, sort_by=['-followers_count'], fields=['_id'], max_offset=top_n)
    sorted_uids = []
    for user in get_results():
        sorted_uids.append(user['_id'])

    return sorted_uids    

@save_to_db
def burst_active_task(top_n, date, window_size):
    date_time = datetime2ts(date)
    uid_active = {}

    db_name = get_leveldb('active', date_time)
    previous_db_name = get_leveldb('active', date_time-window2time(window_size))
    daily_user_active_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, db_name),
                                               block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    previous_daily_user_active_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, previous_db_name),
                                                        block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))  
        
    for uid, active in daily_user_active_bucket.RangeIter():
        uid = int(uid)
        active = float(active)
        try:
            previous_active = float(previous_daily_user_active_bucket.Get(str(uid)))
        except KeyError:
            previous_active = 0
        uid_active[uid] = math.fabs(active - previous_active)

    if len(uid_active) < 100000000:
        sorted_uid_active = sorted(uid_active.iteritems(), key=operator.itemgetter(1), reverse=True)
        sorted_uids = []
        count = 0
        for uid, value in sorted_uid_active:
            if count >= top_n:
                break
            sorted_uids.append(uid)
        count += 1
    else:
        sorted_uids = user_rank(uid_active, 'whole_active', top_n, date, window_size)

    return sorted_uids

@save_to_db
def burst_important_task(top_n, date, window_size):
    date_time = datetime2ts(date)
    uid_important = {}

    db_name = get_leveldb('important', date_time)
    previous_db_name = get_leveldb('important', date_time-window2time(window_size))
    daily_user_important_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, db_name),
                                                  block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    previous_daily_user_important_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, previous_db_name),
                                                           block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

    for uid, important in daily_user_important_bucket.RangeIter():
        uid = int(uid)
        important = float(important)
        try:
            previous_important = float(previous_daily_user_important_bucket.Get(str(uid)))
        except:
            previous_important = 0
        uid_important[uid] = math.fabs(important - previous_important)

    if len(uid_important) < 100000000:
        sorted_uid_important = sorted(uid_important.iteritems(), key=operator.itemgetter(1), reverse=True)
        sorted_uids = []
        count = 0
        for uid, value in sorted_uid_important:
            if count >= top_n:
                break
            sorted_uids.append(uid)
        count += 1
    else:
        sorted_uids = user_rank(uid_important, 'whole_active', top_n, date, window_size)

    return sorted_uids

def task(window_size=1):
    # current_time = time.time()
    current_time = datetime2ts('2013-3-7')
    current_date = ts2datetime(current_time)
    date = current_date
    top_n = 5000
    
    whole_followers_task(top_n, date, window_size)

    whole_active_task(top_n, date, window_size)

    whole_important_task(top_n, date, window_size)

    burst_followers_task(top_n, date, window_size)

    burst_active_task(top_n, date, window_size)

    burst_important_task(top_n, date, window_size)

if __name__ == '__main__':
    task()
