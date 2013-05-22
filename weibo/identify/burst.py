# -*- coding: utf-8 -*-

import os
import time
import math
import operator
from datetime import datetime
import leveldb

from utils import save_rank_results, is_in_black_list
from time_utils import datetime2ts, window2time
from config import FOLLOWERS_MIN_SUPPORT, REPOSTS_MIN_SUPPORT

from xapian_weibo.xapian_backend import XapianSearch

LEVELDBPATH = '/home/mirage/leveldb'

def followers_rank(top_n, date, window_size):
    user_search = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_user', schema_version=1)
    count, get_results = user_search.search(query={'followers_count': {'$gt': FOLLOWERS_MIN_SUPPORT}}, sort_by=['-followers_count'], fields=['_id'], max_offset=top_n)
    sorted_uids = []
    for user in get_results():
        sorted_uids.append(user['_id'])
    data = save_rank_results(sorted_uids, 'burst', 'followers', date, window_size)
    return data

def active_rank(top_n, date, window_size):
    date_time = datetime2ts(date)

    if window_size == 1:
        db_name = get_leveldb('active', date_time)
        previous_db_name = get_leveldb('active', date_time-window2time(window_size))
        daily_user_active_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, db_name),
                                              block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
        previous_daily_user_active_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, previous_db_name),
                                                   block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))  
        uid_active = {}
        for uid, active in daily_user_active_bucket.RangeIter():
            active = float(active)
            if is_in_black_list(uid):
                continue
            try:
                previous_active = float(previous_daily_user_active_bucket.Get(uid))
            except:
                previous_active = 0
            uid_active[int(uid)] = math.fabs(active - previous_active)

        sorted_uid_active = sorted(uid_active.iteritems(), key=operator.itemgetter(1), reverse=True)
        sorted_uids = []
        count = 0
        for uid, value in sorted_uid_active:
            if count >= top_n:
                break
            sorted_uids.append(uid)
            count += 1

    data = save_rank_results(sorted_uids, 'burst', 'active', date, window_size)

    return data

def important_rank(top_n, date, window_size):
    date_time = datetime2ts(date)

    if window_size == 1:
        db_name = get_leveldb('important', date_time)
        previous_db_name = get_leveldb('important', date_time-window2time(window_size))
        daily_user_important_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, db_name),
                                              block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
        previous_daily_user_important_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, previous_db_name),
                                                   block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))  
        uid_important = {}
        for uid, important in daily_user_important_bucket.RangeIter():
            important = float(important)
            if is_in_black_list(uid):
                continue
            try:
                previous_important = float(previous_daily_user_important_bucket.Get(uid))
            except:
                previous_important = 0
            uid_important[int(uid)] = math.fabs(important - previous_important)

        sorted_uid_important = sorted(uid_important.iteritems(), key=operator.itemgetter(1), reverse=True)
        sorted_uids = []
        count = 0
        for uid, value in sorted_uid_important:
            if count >= top_n:
                break
            sorted_uids.append(uid)
            count += 1

    data = save_rank_results(sorted_uids, 'burst', 'important', date, window_size)

    return data

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
