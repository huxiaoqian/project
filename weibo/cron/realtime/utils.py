# -*- coding: utf-8 -*-

import os
import sys
import time
import redis
import leveldb
import datetime
from config import REDIS_HOST, REDIS_PORT, LEVELDBPATH

NOW_DB_START_TS = "now_db_start_ts"  # start ts
LAST_COMPLETE_START_TS = "last_complete_start_ts" # last complete


def _default_redis(host=REDIS_HOST, port=REDIS_PORT, db=0):
    return redis.StrictRedis(host, port, db)


def check_profile_person_keywords(now_datestr):
    has_profile_person_keywords = True 
    try:
        daily_profile_keywords_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_person_keywords_%s' % now_datestr),
                                                        block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    except Exception, e:
        print e
    	has_profile_person_keywords = False
        sys.exit('%s profile keywords are not available now' % now_datestr)

    return has_profile_person_keywords


def check_xapian_completed(now_datestr):
    # 23:45
    xapian_completed_ts = int(time.mktime(time.strptime(now_datestr + ' ' + '23:45:00', '%Y%m%d %H:%M:%S')))
    xapian_completed = False

    global_r0 = _default_redis()
    now_db_start_ts = global_r0.get(NOW_DB_START_TS)
    last_complete_start_ts = global_r0.get(LAST_COMPLETE_START_TS)

    if now_db_start_ts and last_complete_start_ts:
        now_db_start_ts = int(now_db_start_ts)
        last_complete_start_ts = int(last_complete_start_ts)

        if now_db_start_ts >= xapian_completed_ts and last_complete_start_ts >= xapian_completed_ts:
            xapian_completed = True  
    
    return xapian_completed


if __name__ == '__main__':
    now_datestr = sys.argv[1]

    print check_profile_person_keywords(now_datestr)
    print check_xapian_completed(now_datestr)
