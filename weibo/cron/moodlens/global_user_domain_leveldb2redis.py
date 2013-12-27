# -*- coding: utf-8 -*-

import os
import time
import redis
import leveldb
from config import fields_value, REDIS_HOST, REDIS_PORT, LEVELDBPATH

USER_DOMAIN = "user_domain" # user domain hash
UPDATE_TIME = '20131220'
try:
	spieduser_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'spiedusers'),
                                 	   block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
except:
	print 'leveldb not available now'
labels = ['university', 'homeadmin', 'abroadadmin', 'homemedia', 'abroadadmin', 'folkorg', \
          'lawyer', 'politician', 'mediaworker', 'activer', 'grassroot', 'other']
zh_labels = ['高校微博', '境内机构', '境外机构', '媒体', '境外媒体', '民间组织', '律师', \
             '政府官员', '媒体人士', '活跃人士', '草根', '其它']
DOMAIN_LIST = fields_value + labels


def _default_redis(host=REDIS_HOST, port=REDIS_PORT, db=0):
    return redis.StrictRedis(host, port, db)


def _add_all_user_domain(r):
    '''test 10000 users per second
    '''
    count = 0
    ts = te = time.time()
    for k, v in spieduser_bucket.RangeIter():
        uid, updatetime = k.split('_')
        uid = int(uid)
        domainid = DOMAIN_LIST.index(v)
        r.hset(USER_DOMAIN, uid, domainid)

        count += 1
        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts)
            ts = te


def _clear_all_user_domain(r):
    '''test 3000 users per second
    '''
    next_cursor, user_dicts = r.hscan(USER_DOMAIN)
    count = 0
    ts = te = time.time()
    while next_cursor != '0':
        for k in user_dicts.keys():
            r.hdel(USER_DOMAIN, k)
        next_cursor, user_dicts = r.hscan(USER_DOMAIN, next_cursor)        

        count += 10
        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts)
            ts = te


def _scan_all_user_domain(r):
    next_cursor, user_dicts = r.hscan(USER_DOMAIN)
    while next_cursor != '0':
        next_cursor, user_dicts = r.hscan(USER_DOMAIN, next_cursor)
        for k in user_dicts.keys():
            print k, type(k), user2domain(r, k), type(user2domain(r, k))


def user2domain(r, uid):
    domainid = r.hget(USER_DOMAIN, str(uid))
    if not domainid:
        domainid = -1 # not taged label
    
    return int(domainid)


if __name__ == '__main__':
	r = _default_redis()

	#_add_all_user_domain(r)
	#_clear_all_user_domain(r)
	_scan_all_user_domain(r)
