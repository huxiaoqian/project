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

LEVELDBPATH = '/home/mirage/leveldb'
field_daily_location_count_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_field_daily_location_count'),
                                                    block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

fields_value = ['culture', 'education', 'entertainment', 'fashion', 'finance', 'media', 'sports', 'technology']

province_city_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_province_city'),
                                       block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

'''
def getLocationID(uid, location):
    loc = location.split(' ')
    if len(loc) == 1:
        province_str = loc[0]
        city_str = '不限'
    if len(loc) == 2:
        province_str = loc[0]
        city_str = loc[1]
    try:
        province_id = province_city_bucket.Get('province_' + province_str)
    except KeyError:
        province_id = 'none'
    try:
        city_id = province_city_bucket.Get('city_' + city_str)
    except KeyError:
        city_id = 'none'
        
    if province_id == 'none':
        from getReadonlyDB import getReadonlyDB
        mongo_master_timeline = getReadonlyDB('master_timeline')
        user = mongo_master_timeline.master_timeline_user.find_one({'_id': int(uid)})
        if user:
            if city_id == 'none':
                try:
                    city_id = str(user['city'])
                    province_city_bucket.Put('city_' + city_str, city_id)
                except KeyError:
                    city_id = 'none'
            if province_id == 'none':
                try:
                    province_id = str(user['province'])
                    province_city_bucket.Put('province_' + province_str, province_id)
                except KeyError:
                    province_id = 'none'

    return province_id 
    #return {'province_id': province_id, 'city_id': city_id}
'''

def getUidsByFieldFromRedis(field, update_date='20130430', sorted_key='followers_count'):
    sorted_set_key = 'linhao_dailyfielduser_%s_%s_%s' % (update_date, field, sorted_key)
    start = 0
    end = redis_conn.zcard(sorted_set_key)-1
    results = redis_conn.zrange(sorted_set_key, start, end)
    uids = results
    return uids

def initial_load_field_location(update_date='20130430'):
    sys.path.append('../../profile')
    from getReadonlyDB import getReadonlyDB
    mongo_master_timeline = getReadonlyDB('master_timeline')

    count = 0
    ts = te = time.time()
    daily_count_kv = {}
    for field in fields_value:
        uids = getUidsByFieldFromRedis(field)
        for uid in uids:
            count += 1
            if count % 10000 == 0:
                te = time.time()
                print count, '%s sec' % (te - ts)
                ts = te
            user = mongo_master_timeline.master_timeline_user.find_one({'_id': int(uid)})
            if user:
                try:
                    location = user['location']
                    province_str = str(location.split(' ')[0].encode('utf-8'))
                    province_id = str(user['province'])
                    province_city_bucket.Put('province_' + province_str, province_id)
                    province_city_bucket.Put('provinceid_' + province_id, province_str)
                except KeyError:
                    continue
            else:
                continue
            try:
                v_count = daily_count_kv[str(field) + '_' + str(update_date) + '_' + str(province_id)]
                daily_count_kv[str(field) + '_' + str(update_date) + '_' + str(province_id)] = str(int(v_count) + 1)
            except KeyError:
                daily_count_kv[str(field) + '_' + str(update_date) + '_' + str(province_id)] = str(1)

    print len(daily_count_kv.keys())
    
    batch = leveldb.WriteBatch()
    for k, v in daily_count_kv.iteritems():
        batch.Put(k, str(v))
    field_daily_location_count_bucket.Write(batch, sync=True)

if __name__ == '__main__':
    initial_load_field_location()


