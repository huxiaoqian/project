# -*- coding: utf-8 -*-

import os
import time
import operator

import leveldb

from datetime import datetime

from utils import is_in_trash_list, acquire_user_by_id, user_status
from config import BURST_MIN_SUPPORT

LEVELDBPATH = '/home/mirage/leveldb'

def realtime_burst_user(top_n, current_time):
    current_datetime = datetime.fromtimestamp(current_time)
    current_hour = current_datetime.hour
    total_size = 0
    total_uid_count = {}
    for h in range(current_hour):
        db_name = get_leveldb(current_time, h)
        uid_group_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, db_name),
                                              block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
        try:
            group_size = uid_group_bucket.Get('size')
        except KeyError:
            continue
        if group_size:
            group_size = int(group_size)
            if group_size > 0:
                total_size += group_size
        for uid, value in uid_group_bucket.RangeIter():
            if uid == 'size':
                continue
            uid = int(uid)
            value = float(value)
            if uid not in total_uid_count:
                total_uid_count[uid] = 0
            total_uid_count[uid] += value 

    uid_burst = {}
    for h in range(current_hour):
        db_name = get_leveldb(current_time, h)
        uid_group_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, db_name),
                                              block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
        try:
            group_size = uid_group_bucket.Get('size')
        except KeyError:
            continue
        if group_size:
            group_size = int(group_size)
            if group_size <= 0:
                continue
        for uid, value in uid_group_bucket.RangeIter():
            if uid == 'size':
                continue
            uid = int(uid)
            value = float(value)
            A = value
            B = total_uid_count[uid] - A
            C = group_size - A
            D = total_size - total_uid_count[uid] - C
            if uid not in uid_burst:
                uid_burst[uid] = 0
            uid_burst[uid] += (A + B + C + D) * ((A*D - B*C) ** 2) * 1.0 / ((A + B) * (C + D) * (A + C) * (B + D))
    
    sorted_uid_burst = sorted(uid_burst.iteritems(), key=operator.itemgetter(1), reverse=True)
    sorted_uids = []
    count = 0
    for uid, value in sorted_uid_burst:
        if is_in_trash_list(uid):
            continue
        if count >= top_n:
            break
        sorted_uids.append(uid)
        count += 1

    data = save_rank_results(sorted_uids)
    return data
    
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

def save_rank_results(sorted_uids):
    data = []
    rank = 1
    for uid in sorted_uids:
        user = acquire_user_by_id('whole', uid)
        if not user:
            continue
        name = user['name']
        location = user['location']
        count1 = user['count1']
        count2 = user['count2']
        #read from external knowledge database
        status = user_status(uid)
        row = (rank, uid, name, location, count1, count2, status)
        data.append(row)
        rank += 1
    return data

def main():
    for uid in realtime_burst_user(top_n):
        print uid

if __name__ == '__main__': main()
