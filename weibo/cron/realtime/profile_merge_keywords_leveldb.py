# -*- coding: utf-8 -*-


import os
import sys
import redis
import time
import zlib
import json
import cPickle as pickle
import leveldb
from config import LEVELDBPATH, REDIS_HOST, REDIS_PORT


KEYWORDS_DB_COMPLETED = "profile_daily_keywords_db:%s" # date as '20140311'


def _default_redis(host=REDIS_HOST, port=REDIS_PORT, db=0):
    return redis.StrictRedis(host, port, db)


def merge_keywords_leveldb():
    while 1:
        print 'sleep 60 seconds %s' % now_datestr
        time.sleep(60)
        files_names_list = os.listdir(os.path.join(LEVELDBPATH, 'keywords'))
        for f in files_names_list:
            if f[24:32] != now_datestr:
                continue

            completed_tasks = global_r0.smembers(KEYWORDS_DB_COMPLETED % now_datestr)
            if f in completed_tasks:
                continue

            print f, ' start merge to %s keywords leveldb' % now_datestr
          
            try:
    	        one_ldb = leveldb.LevelDB(os.path.join(LEVELDBPATH, './keywords/' + f),
    	                                  block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
                print os.path.join(LEVELDBPATH, './keywords/' + f)
            except Exception, e:
                print e
    	        continue

            count = 0
            ts = te = time.time()
            for k, v in one_ldb.RangeIter():
                uid = int(k)
                kcount = pickle.loads(zlib.decompress(v))

                try:          
                    _kcount = json.loads(daily_profile_keywords_bucket.Get(k))
                except:
                    _kcount = {}

                for _k, _v in kcount:
                    try:
                        _kcount[_k] += int(_v)
                    except KeyError:
                        _kcount[_k] = int(_v)

                daily_profile_keywords_bucket.Put(k, json.dumps(_kcount))
                if count % 10000 == 0:
                    te = time.time()
                    print count, '%s sec' % (te - ts), now_datestr, 'merge %s' % f
                    ts = te
                count += 1

            global_r0.sadd(KEYWORDS_DB_COMPLETED % now_datestr, f)
            
            if f[33:] == '0':
                sys.exit('complete today job %s' % now_datestr)        


if __name__ == '__main__':
    now_datestr = sys.argv[1]
    try:
        daily_profile_keywords_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_person_keywords_%s' % now_datestr),
                                                        block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    except:
        sys.exit('%s profile keywords cant merge now' % now_datestr)

    global_r0 = _default_redis()

    merge_keywords_leveldb()
