# -*- coding: utf-8 -*-

import os
import sys
import redis
import shutil
import leveldb
from config import LEVELDBPATH, REDIS_HOST, REDIS_PORT

KEYWORDS_DB_COMPLETED = "profile_daily_keywords_db:%s" # date as '20140311'


def _default_redis(host=REDIS_HOST, port=REDIS_PORT, db=0):
    return redis.StrictRedis(host, port, db)


def clear_profile_person_keywords_db():
    files_names_list = os.listdir(os.path.join(LEVELDBPATH, 'keywords'))
    for f in files_names_list:
        if f[24:32] != now_datestr:
            continue

        shutil.rmtree(os.path.join(LEVELDBPATH, 'keywords/%s' % f))


def clear_keywords_task_redis_db():
	  _default_redis().delete(KEYWORDS_DB_COMPLETED % now_datestr)


if __name__ == '__main__':
	  now_datestr = sys.argv[1]
	  clear_profile_person_keywords_db()
	  clear_keywords_task_redis_db()
