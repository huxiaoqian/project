# -*- coding: utf-8 -*-
import random
import time
import os
import leveldb
import redis
from time_utils import ts2datetime, datetime2ts
from xapian_weibo.xapian_backend import XapianSearch
from config import LEVELDBPATH, USER_DOMAIN, REDIS_HOST, REDIS_PORT, DOMAIN_LIST
from dynamic_xapian_weibo import getXapianWeiboByDate


def _default_redis(host=REDIS_HOST, port=REDIS_PORT, db=0):
    return redis.StrictRedis(host, port, db)


def user2domain(uid):
    global_r0 = _default_redis()
    domainid = global_r0.hget(USER_DOMAIN, str(uid))
    if not domainid:
        domainid = -1 # not taged label
    
    return int(domainid)


def user2domainFromLeveldb(uid, updatetime='20131220'):
    try:
        domainstr = user_domain_bucket.Get(str(uid) + '_' + str(updatetime))
        domainid = DOMAIN_LIST.index(domainstr)
    except:
        domainid = -1

    return domainid


def user2FollowersCount(uid):
    try:
        followers_count = int(daily_followers_count_bucket.Get(str(uid)))
    except KeyError:
        followers_count = 0
 
    return followers_count


def read_xapian(date):
    # init leveldb
    print 'init leveldb'
    dailycount_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'yuanshi_daily_count_%s' % date),
                                        block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

    
    print 'init xapian weibo'
    # init xapian weibo
    datestr = date.replace('-', '')
    xapian_search_weibo = getXapianWeiboByDate(datestr)

    if not xapian_search_weibo:
        return 'wrong'
    
    # iter
    weibos = xapian_search_weibo.iter_all_docs(fields=['user', 'reposts_count', 'comments_count', 'attitudes_count'])

    # cal
    count = 0
    te = ts = time.time()
    for weibo in weibos:
        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts), 'identify weibo calc to leveldb ', date
            ts = te
        count += 1

        try:
            reposts_count = int(weibo['reposts_count'])
        except:
            reposts_count = 0

        try:
            comments_count = int(weibo['comments_count'])
        except:
            comments_count = 0

        try:
            attitudes_count = int(weibo['attitudes_count'])
        except:
            attitudes_count = 0

        uid = weibo['user']

        try:
            active, important, follower, domain = dailycount_bucket.Get(str(uid)).split('_')
            active = int(active)
            important = int(important)
            active += 1
            important += reposts_count + comments_count + attitudes_count
        except KeyError:
            active = 1
            important = reposts_count + comments_count + attitudes_count
            domain = user2domainFromLeveldb(uid)
            follower = user2FollowersCount(uid)

        dailycount_bucket.Put(str(uid), str(active) + '_' + str(important) + '_' + str(follower) + '_' + str(domain))
    
    return 'Done'


if __name__ == "__main__":
    import sys
    date = sys.argv[1] # '2013-09-06'
    num = sys.argv[2]
    daily_followers_count_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'yuanshi_daily_user_followers_%s' % num),
                                               block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    user_domain_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_user2domain_20140112_%s' % num),
                                     block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    answer = read_xapian(date)
    print answer
