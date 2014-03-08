# -*- coding: utf-8 -*-

import os
import time
import zlib
import redis
import leveldb
import datetime
import cPickle as pickle
from config import REDIS_HOST, REDIS_PORT, LEVELDBPATH
from xapian_weibo.utils import get_now_db_no


USER_KEYWORDS = "user_keywords_%s" # user keywords sorted set, uid,
USER_SET = "user_profile" # user set,
LEVELDB_CONF_MAX_DB_NO = 96

def _default_redis(host=REDIS_HOST, port=REDIS_PORT, db=0):
    return redis.StrictRedis(host, port, db)


def profile_keywords_redis2leveldb():
    cursor, members = r.sscan(USER_SET, cursor=0, count=10000)
    count = 0
    ts = te = time.time()
    while cursor != "0":
        for uid in members:
            if count % 10000 == 0:
        	te = time.time()
                print count, '%s sec' % (te - ts), ' profile_keywords_redis2leveldb', now_db_no, now_leveldb_no, now_datestr
                ts = te
            count += 1

            # print uid
            keywords_with_count = r.zrange(USER_KEYWORDS % uid, 0, -1, withscores=True)
            daily_profile_keywords_bucket.Put(str(uid), zlib.compress(pickle.dumps(keywords_with_count, pickle.HIGHEST_PROTOCOL), zlib.Z_BEST_COMPRESSION))

        cursor, members = r.sscan(USER_SET, cursor=cursor, count=10000)


def ts_div_fifteen_m():
    return int(time.time()) / (15 * 60)


def get_now_leveldb_no():
    return ts_div_fifteen_m() % LEVELDB_CONF_MAX_DB_NO + 1


def get_now_datestr():
    return datetime.datetime.now().strftime("%Y%m%d")
        

if __name__ == '__main__':
    # init redis
    now_db_no = get_now_db_no()
    print "redis db no now", now_db_no
    r = _default_redis(db=now_db_no)
    
    # init leveldb
    now_datestr = get_now_datestr()
    now_leveldb_no = get_now_leveldb_no()
    print "leveldb no now", now_leveldb_no, now_datestr
    daily_profile_keywords_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, './keywords/linhao_profile_keywords_%s_%s' % (now_datestr, now_leveldb_no)),
                                                    block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

    profile_keywords_redis2leveldb()
