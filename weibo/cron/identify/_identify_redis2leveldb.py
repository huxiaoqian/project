# -*- coding: utf-8 -*-

import redis
import heapq
import datetime
from model import WholeIdentification
from time_utils import datetimestr2ts, ts2datetime
from config import REDIS_HOST, REDIS_PORT, LEVELDBPATH, fields_value, labels, db


CAL_TOPK = 1000
SAVE_TOPK = 1000
DOMAINS_LIST = fields_value + labels
USER_DOMAIN = "user_domain" # user domain hash,
GLOBAL_ACTIVE_COUNT = "global_active_%s" # date as '20131227',
GLOBAL_IMPORTANT_COUNT = "global_important_%s" # date as '20131227',
DOMAIN_ACTIVE_COUNT = "domain_active_%s:%s" # date as '20131227', domain
DOMAIN_IMPORTANT_COUNT = "domain_important_%s:%s" # date as '20131227', domain
user_followers_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'yuanshi_daily_user_followers'),
                                        block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))


def get_daily_user_count_db_by_date(datestr):
    # datestr '20140105'
    daily_user_count_db = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'yuanshi_daily_count_%s' % datestr),
                                          lock_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    return daily_user_count_db


def _default_redis(host=REDIS_HOST, port=REDIS_PORT, db=0):
    return redis.StrictRedis(host, port, db)


def get_now_datestr():
    return datetime.datetime.now().strftime("%Y%m%d")


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


def redis2leveldb():
    next_cursor, result_dicts = global_r0.hscan(GLOBAL_ACTIVE_COUNT % now_datestr)
    count = 0
    ts = te = time.time()
    while next_cursor != '0':
        for uid, active in result_dicts.iteritems():
            followers_count = getUserFollowerscount(uid)
            important = getUserImportant(uid)
            domain = user2domain(uid)
            key = str(uid)
            value = str(active) + '_' + str(important) + '_' + str(followers_count) + '_' + str(domain)
            global_leveldb.Put(key, value)

        next_cursor, result_dicts = global_r0.hscan(GLOBAL_ACTIVE_COUNT % now_datestr)

        count += 10
        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts)
            ts = te
        

if __name__ == '__main__':
    now_datestr = get_now_datestr()
    global_r0 = _default_redis()
    global_leveldb = get_daily_user_count_db_by_date(now_datestr)

    redis2leveldb()