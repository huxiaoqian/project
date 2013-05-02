# -*- coding: utf-8 -*-

import redis
import datetime
import time
import os
import sys

redis_host = 'localhost'
redis_port = 6379
redis_conn = redis.Redis(redis_host, redis_port)

def initial_load_friendship_from_mongo():
    sys.path.append('../../profile')
    from getReadonlyDB import getReadonlyDB
    mongo_master_timeline = getReadonlyDB('master_timeline')

    total_count = mongo_master_timeline.master_timeline_user.find({'$or': [{'friends': {'$ne':[]}}, {'followers': {'$ne':[]}}]}).count()
    print 'hit count from mongodb: ', total_count
    cursor = mongo_master_timeline.master_timeline_user.find({'$or': [{'friends': {'$ne':[]}}, {'followers': {'$ne':[]}}]})

    count = 0
    ts = te = time.time()
    pipe = redis_conn.pipeline()
    pipe.multi()
    for user in cursor:
        friends = user['friends']
        followers = user['followers']
        uid = user['_id']
        if friends and len(friends):
            key_prefix = 'linhao_friends_' + str(uid)
            pipe.sadd(key_prefix, friends)
        if followers and len(followers):
            key_prefix = 'linhao_followers_' + str(uid)
            pipe.sadd(key_prefix, followers)
        pipe.execute()
        count += 1
        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts)
            ts = te
            
if __name__ == '__main__':
    initial_load_friendship_from_mongo()


