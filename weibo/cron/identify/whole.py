# -*- coding: utf-8 -*-

import os
import time
import operator
from datetime import datetime
import leveldb

from utils import save_rank_results, is_in_trash_list
from time_utils import datetime2ts, window2time
from config import FOLLOWERS_MIN_SUPPORT, REPOSTS_MIN_SUPPORT

from user_sorter import user_rank

from xapian_weibo.xapian_backend import XapianSearch
from global_config import xapian_search_user as user_search, LEVELDBPATH
##LEVELDBPATH = '/home/mirage/leveldb'

def save_to_db(method):
    def func(*args, **kw):
        data = method(*args, **kw)
        method_name = method.__name__
        rank_field = method_name.split('_')[0]
##        try:
##            print 'yuan'
        save_rank_results(data, 'whole', rank_field, args[1], args[2])
##        except:
##            print '%s save results failed, it is ok if not in web environment.' % method.__name__
        return data
    return func

@save_to_db
def followers_rank(top_n, date, window_size):
    #user_search = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_user', schema_version=1)
    count, get_results = user_search.search(query={'followers_count': {'$gt': FOLLOWERS_MIN_SUPPORT}}, sort_by=['-followers_count'], fields=['_id'], max_offset=top_n)
    sorted_uids = []
    print count
    for user in get_results():
        sorted_uids.append(user['_id'])

    return sorted_uids

@save_to_db
def active_rank(top_n, date, window_size):
    date_time = datetime2ts(date)
    uid_active = {}
    if window_size == 1:
        db_name = get_leveldb('active', date_time)
        print db_name
        daily_user_active_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, db_name),
                                              block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

        count = 0
        for uid, active in daily_user_active_bucket.RangeIter():
            count = count + 1
            print count
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
            if is_in_trash_list(uid):
                continue
            if count >= top_n:
                break
            sorted_uids.append(uid)
        count += 1
    else:
        sorted_uids = user_rank(uid_active, 'whole_active', top_n, date, window_size)

    return sorted_uids

@save_to_db
def important_rank(top_n, date, window_size):
    date_time = datetime2ts(date)
    uid_important = {}
    if window_size == 1:
        db_name = get_leveldb('important', date_time)
        daily_user_important_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, db_name),
                                              block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

        count = 0
        for uid, important in daily_user_important_bucket.RangeIter():
            count = count + 1
            print count
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
            if is_in_trash_list(uid):
                continue
            if count >= top_n:
                break
            sorted_uids.append(uid)
        count += 1
    else:
        sorted_uids = user_rank(uid_important, 'whole_active', top_n, date, window_size)


    return sorted_uids

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
