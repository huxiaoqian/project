# -*- coding: utf-8 -*-

import datetime
import time
import os
import sys
import leveldb
import json

LEVELDBPATH = '/home/mirage/leveldb'
friendship_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_friendship'),
                                                  block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

def initial_load_friendship_from_mongo():
    import redis
    redis_host = 'localhost'
    redis_port = 6379
    redis_conn = redis.Redis(redis_host, redis_port)
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

def clear_friendship_from_redis():
    import redis
    redis_host = 'localhost'
    redis_port = 6379
    redis_conn = redis.Redis(redis_host, redis_port)

    sys.path.append('../../profile')
    from getReadonlyDB import getReadonlyDB
    mongo_master_timeline = getReadonlyDB('master_timeline')

    total_count = mongo_master_timeline.master_timeline_user.find({'$or': [{'friends': {'$ne':[]}}, {'followers': {'$ne':[]}}]}).count()
    print 'hit count from mongodb: ', total_count
    cursor = mongo_master_timeline.master_timeline_user.find({'$or': [{'friends': {'$ne':[]}}, {'followers': {'$ne':[]}}]})

    count = 0
    ts = te = time.time()
    for user in cursor:
        friends = user['friends']
        followers = user['followers']
        uid = user['_id']
        if friends and len(friends):
            key_prefix = 'linhao_friends_' + str(uid)
            redis_conn.delete(key_prefix)
        if followers and len(followers):
            key_prefix = 'linhao_followers_' + str(uid)
            redis_conn.delete(key_prefix)
        count += 1
        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts)
            ts = te

def load_friendship_to_leveldb():
    from xapian_weibo.xapian_backend import XapianSearch
    s_user = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_user', schema_version=1)
    
    query_dict = {
        '_id': {
            '$gt': 0,
        }
    }
    count, get_results = s_user.search(query=query_dict, fields=['_id', 'friends', 'followers'])
    print count

    count = 0
    ts = te = time.time()
    for r in get_results():
        uid = r['_id']
        friends = r['friends']
        followers = r['followers']
        if friends and len(friends):
            k = str(uid) + '_' + 'friends'
            v = json.dumps(friends)
            friendship_bucket.Put(k, str(v))
        if followers and len(followers):
            k = str(uid) + '_' + 'followers'
            v = json.dumps(followers)
            friendship_bucket.Put(k, str(v))
        count += 1
        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts)
            ts = te
            
if __name__ == '__main__':
    #initial_load_friendship_from_mongo()
    #clear_friendship_from_redis()
    load_friendship_to_leveldb()


