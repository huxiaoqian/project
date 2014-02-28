# -*- coding: utf-8 -*-

import sys
import time
import redis
import datetime
from config import REDIS_HOST, REDIS_PORT, GLOBAL_ACTIVE_COUNT, GLOBAL_IMPORTANT_COUNT, \
                   LEVELDBPATH


def _default_redis(host=REDIS_HOST, port=REDIS_PORT, db=0):
    return redis.StrictRedis(host, port, db)


def get_daily_airoeik_db_by_date(datestr):
    # datestr '20140105'
    db = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_person_airoeik_%s' % datestr),
                         block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    return db


def get_now_datestr():
    return datetime.datetime.now().strftime("%Y%m%d")


def get_before_datestr():
    now_ts = time.time() - 24 * 3600
    datestr = datetime.date.fromtimestamp(now_ts).isoformat().replace('-', '')
    return datestr


def active_redis2leveldb(batch_scan_count=10000):
    next_cursor, result_dicts = global_r0.hscan(GLOBAL_ACTIVE_COUNT % now_datestr, cursor=0, count=batch_scan_count)
    count = 0
    ts = te = time.time()
    while next_cursor != '0':
        batch = leveldb.WriteBatch()
        important = 0
        reposts = 0
        original = 0
        emoticon = 0
        interact_dict = {}
        keywords_dict = {}
        for uid, active in result_dicts.iteritems():
            key = str(uid)
            value = str(active) + '_' + str(important) + '_' + str(reposts) + '_' + str(original) + '_' + str(emoticon) + '_' + \
                    str(interact_dict) + '_' + str(keywords_dict)
            batch.Put(key, value)

        global_airoeik_leveldb.Write(batch, sync=True)
        next_cursor, result_dicts = global_r0.hscan(GLOBAL_ACTIVE_COUNT % now_datestr, cursor=next_cursor, count=batch_scan_count)
        
        count += batch_scan_count
        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts), ' active redis 2 leveldb'
            ts = te


def important_redis2leveldb(batch_scan_count=10000):
    next_cursor, result_dicts = global_r0.hscan(GLOBAL_IMPORTANT_COUNT % now_datestr, cursor=0, count=batch_scan_count)
    count = 0
    ts = te = time.time()
    while next_cursor != '0':
        batch = leveldb.WriteBatch()
        for uid, important in result_dicts.iteritems():
            key = str(uid)
            try:
                active, _important, reposts, original, emoticon, interact_dict, keywords_dict = global_airoeik_leveldb.Get(key).split('_')
            except KeyError:
            	active = 0
            	reposts = 0
            	original = 0
            	emoticon = 0
            	interact_dict = {}
            	keywords_dict = {}
            value = str(active) + '_' + str(important) + '_' + str(reposts) + '_' + str(original) + '_' + str(emoticon) + '_' + \
                    str(interact_dict) + '_' + str(keywords_dict)
            batch.Put(key, value)

        global_airoeik_leveldb.Write(batch, sync=True)
        next_cursor, result_dicts = global_r0.hscan(GLOBAL_IMPORTANT_COUNT % now_datestr, cursor=next_cursor, count=batch_scan_count)
        
        count += batch_scan_count
        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts), ' important redis 2 leveldb'
            ts = te


def roeik_xapian2leveldb():
	  


if __name__ == '__main__':
	  # get datestr
    now_datestr = get_now_datestr()

	  # init xapian weibo
    xapian_search_weibo = getXapianWeiboByDate(now_datestr)

	  global_r0 = _default_redis()
	  global_airoeik_leveldb = get_daily_airoeik_db_by_date(now_datestr)
		active_redis2leveldb()

