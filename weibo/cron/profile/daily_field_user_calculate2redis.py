# -*- coding: utf-8 -*-

import leveldb
import redis
import datetime
import time
import os
import sys

redis_host = 'localhost'
redis_port = 6379
redis_conn = redis.Redis(redis_host, redis_port)

fields_value = ['culture', 'education', 'entertainment', 'fashion', 'finance', 'media', 'sports', 'technology']

def initial_load_field_user_from_mysql(update_date='20130430'):
    sys.path.append('../../profile')
    from config import db
    from model import UserField
    from getReadonlyDB import getReadonlyDB
    mongo_master_timeline = getReadonlyDB('master_timeline')

    count = 0
    ts = te = time.time()
    daily_fields_kv = {}
    results = db.session.query(UserField).all()
    for r in results:
        uid = r.uid
        try:
            fields = ','.join([r.fieldFirst, r.fieldSecond])
            if r.fieldFirst not in fields_value or r.fieldSecond not in fields_value:
                count += 1
                continue
        except TypeError,e:
            fields = r.fieldFirst
            if r.fieldFirst not in fields_value:
                count += 1
                continue
        daily_fields_kv[str(uid)] = fields
        count += 1
        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts)
            ts = te
    
    count = 0
    ts = te = time.time()
    pipe = redis_conn.pipeline()
    pipe.multi()
    for k, v in daily_fields_kv.iteritems():
        fieldFirst = v.split(',')[0]
        sorted_key = 'followers_count'
        sortedset_key = 'linhao_dailyfielduser_%s_%s_%s' % (update_date, fieldFirst, sorted_key)
        user = mongo_master_timeline.master_timeline_user.find_one({'_id': int(k)})
        if user:
            sorted_value = user[sorted_key]
        else:
            count += 1
            continue
        if fieldFirst in fields_value:
            pipe.zadd(sortedset_key, str(k), sorted_value)
        count += 1
        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts)
            ts = te
    pipe.execute()

if __name__ == '__main__':
    initial_load_field_user_from_mysql()


