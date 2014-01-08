# -*- coding: utf-8 -*-

import time
import redis
from config import REDIS_HOST, REDIS_PORT, xapian_search_user

USER_NAME_UID = "user_name_uid" # user name-uid hash

def _default_redis(host=REDIS_HOST, port=REDIS_PORT, db=0):
    return redis.StrictRedis(host, port, db)


def user_name_uid_xapian2redis():
    ''' test 2 secondes per 10000
    '''
    count = 0
    ts = te = time.time()
    users = xapian_search_user.iter_all_docs(fields=['name', '_id'])
    for user in users:
        name = user['name']
        uid = user['_id']
        global_r0.hset(USER_NAME_UID, name, int(uid))

        count += 1
        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts)
            ts = te


if __name__ == '__main__':
    global_r0 = _default_redis()
    user_name_uid_xapian2redis()
