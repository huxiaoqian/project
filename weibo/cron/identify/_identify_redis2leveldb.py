# -*- coding: utf-8 -*-

import os
import redis
import time
import leveldb
import datetime
from config import REDIS_HOST, REDIS_PORT, LEVELDBPATH, db

USER_DOMAIN = "user_domain" # user domain hash,
GLOBAL_ACTIVE_COUNT = "global_active_%s" # date as '20131227',
GLOBAL_IMPORTANT_COUNT = "global_important_%s" # date as '20131227',
user_followers_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_daily_user_followers'),
                                        block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))


def get_daily_user_count_db_by_date(datestr):
    # datestr '20140105'
    daily_user_count_db = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'yuanshi_daily_count_%s' % datestr),
                                          block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    return daily_user_count_db


def _default_redis(host=REDIS_HOST, port=REDIS_PORT, db=0):
    return redis.StrictRedis(host, port, db)


def get_now_datestr():
    return datetime.datetime.now().strftime("%Y%m%d")


def get_before_datestr():
    now_ts = time.time() - 24 * 3600
    datestr = datetime.date.fromtimestamp(now_ts).isoformat().replace('-', '')
    return datestr


def getUserActive(uid):
    active = global_r0.hget(GLOBAL_ACTIVE_COUNT % now_datestr, str(uid))
    if not active:
        return 0
    
    return int(active)


def getUserImportant(uid):
    important = global_r0.hget(GLOBAL_IMPORTANT_COUNT % now_datestr, str(uid))
    if not important:
        return 0
    
    return int(important)


def getUserFollowerscount(uid):
    try:
        followers_count = int(user_followers_bucket.Get(str(uid)))
    except KeyError:
        followers_count = 0
 
    return followers_count


def user2domain(uid):
    domainid = global_r0.hget(USER_DOMAIN, str(uid))
    if not domainid:
        domainid = -1 # not taged label
    
    return int(domainid)


def redis2leveldb(batch_scan_count=10000):
    next_cursor, result_dicts = global_r0.hscan(GLOBAL_ACTIVE_COUNT % now_datestr, cursor=0, count=batch_scan_count)
    count = 0
    ts = te = time.time()
    uidset = set()
    while next_cursor != '0':
        batch = leveldb.WriteBatch()
        for uid, active in result_dicts.iteritems():
            followers_count = getUserFollowerscount(uid)
            important = getUserImportant(uid)
            domain = user2domain(uid)
            key = str(uid)
            value = str(active) + '_' + str(important) + '_' + str(followers_count) + '_' + str(domain)
            batch.Put(key, value)
            uidset.add(key)
        global_leveldb.Write(batch, sync=True)
        print next_cursor
        print len(uidset)
        next_cursor, result_dicts = global_r0.hscan(GLOBAL_ACTIVE_COUNT % now_datestr, cursor=next_cursor, count=batch_scan_count)
        
        count += batch_scan_count
        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts)
            ts = te


def test_read():
    count = 0
    for k, v in global_leveldb.RangeIter():
        count += 1
    print count
        

if __name__ == '__main__':
    now_datestr = get_before_datestr()
    global_r0 = _default_redis()
    global_leveldb = get_daily_user_count_db_by_date(now_datestr)

    redis2leveldb()
    #test_read()
