# -*- coding: utf-8 -*-

from xapian_weibo.xapian_backend import XapianSearch
import leveldb
import datetime
import time
import os

s = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline')

LEVELDBPATH = '/home/mirage/leveldb'
weibo_is_retweet_status_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'lijun_weibo_is_retweet_status'),
                                                 block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
user_daily_post_count_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_user_daily_post_count'),
                                               block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

post_status_kv = {'total': 2, 'repost': 1, 'fipost': 0}

def initial_load_user_post_count():
    total_days = 89
    today = datetime.datetime.today()
    now_ts = time.mktime(datetime.datetime(today.year, today.month, today.day, 2, 0).timetuple())
    now_ts = int(now_ts)
    during = 24 * 3600
    begin_ts = now_ts - total_days * during

    query_dict = {
        'timestamp': {'$gt': begin_ts, '$lt': now_ts}
    }
    count, get_results = s.search(query=query_dict, fields=['user', 'id', 'timestamp'])
    print count

    count = 0
    ts = te = time.time()
    daily_post_count_kv = {}
    for r in get_results():
        if 'user' in r and r['user'] and 'id' in r['user']:
            uid = r['user']['id']
        else:
            count += 1
            continue
        id_str = str(r['id'])

        try:
            weibo_is_retweet_status = weibo_is_retweet_status_bucket.Get(id_str)
        except KeyError,e:
            count += 1
            continue

        if weibo_is_retweet_status:
            weibo_is_retweet_status = int(weibo_is_retweet_status)
        if weibo_is_retweet_status in [0, 1]:
            for i in xrange(-total_days + 1, 1):
                gt = now_ts + during * (i - 1)
                lt = now_ts + during * i
                if gt < r['timestamp'] <= lt:
                    try:
                        p_count = daily_post_count_kv[str(uid) + '_' + str(lt) + '_' + str(weibo_is_retweet_status)]
                        daily_post_count_kv[str(uid) + '_' + str(lt) + '_' + str(weibo_is_retweet_status)] = str(int(p_count) + 1)
                    except KeyError,e:
                        daily_post_count_kv[str(uid) + '_' + str(lt) + '_' + str(weibo_is_retweet_status)] = str(1)

        count += 1
        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts)
            ts = te
    
    batch = leveldb.WriteBatch()
    for k, v in daily_post_count_kv.iteritems():
        batch.Put(k, str(v))
    user_daily_post_count_bucket.Write(batch, sync=True)

if __name__ == '__main__':
    initial_load_user_post_count()


