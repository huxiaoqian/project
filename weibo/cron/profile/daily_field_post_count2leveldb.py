# -*- coding: utf-8 -*-

from xapian_weibo.xapian_backend import XapianSearch
import leveldb
import datetime
import time
import os
import redis

s = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline')
LEVELDBPATH = '/home/mirage/leveldb'

user_daily_post_count_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_user_daily_post_count'),
                                               block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
user_daily_field_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_user_daily_field'),
                                          block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
field_daily_post_count_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_field_daily_post_count'),
                                                block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

post_status_kv = {'total': 2, 'repost': 1, 'fipost': 0}
fields_value = ['culture', 'education', 'entertainment', 'fashion', 'finance', 'media', 'sports', 'technology']

def main():
    count = 0
    ts = te = time.time()
    daily_count_kv = {}
    results = user_daily_post_count_bucket.RangeIter()
    for k, v in results:
        count += 1
        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts)
            ts = te
        uid, lt, is_retweeted = k.split('_')
        try:
            u_field = user_daily_field_bucket.Get('%s_20130430' % uid).split(',')[0]
            try:
                p_count = daily_count_kv[str(u_field) + '_' + str(lt) + '_' + str(is_retweeted)]
                daily_count_kv[str(u_field) + '_' + str(lt) + '_' + str(is_retweeted)] = str(int(p_count) + int(v))
            except KeyError:
                daily_count_kv[str(u_field) + '_' + str(lt) + '_' + str(is_retweeted)] = str(v)
        except KeyError,e:
            continue
        
    print len(daily_count_kv.keys())

    batch = leveldb.WriteBatch()
    for k, v in daily_count_kv.iteritems():
        batch.Put(k, str(v))
    field_daily_post_count_bucket.Write(batch, sync=True)

if __name__ == '__main__':
    main()
