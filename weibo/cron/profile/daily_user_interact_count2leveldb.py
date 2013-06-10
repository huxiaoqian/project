# -*- coding: utf-8 -*-

from xapian_weibo.xapian_backend import XapianSearch
import leveldb
import datetime
import time
import os
import re

statuses_search = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_weibo', schema_version=2)
user_search = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_user', schema_version=1)

LEVELDBPATH = '/home/mirage/leveldb'
user_daily_interact_count_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_user_daily_interact_count'),
                                                   block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

def initial_load_user_interact_count():
    total_days = 89
    today = datetime.datetime.today()
    now_ts = time.mktime(datetime.datetime(today.year, today.month, today.day, 2, 0).timetuple())
    now_ts = int(now_ts)
    during = 24 * 3600
    begin_ts = now_ts - total_days * during

    query_dict = {
        'timestamp': {'$gt': begin_ts, '$lt': now_ts}
    }
    count, get_results = statuses_search.search(query=query_dict, fields=['user', 'text', 'retweeted_status', 'timestamp'])
    print count

    count = 0
    ts = te = time.time()
    daily_interact_count_kv = {}
    for r in get_results():
        uid = r['user']
        direct_uid = None
        nicknames = re.findall(r'//@(\S+?):', r['text'])
        if len(nicknames) and nicknames:
            u_count, u_results = user_search.search(query={'name': nicknames[0]}, fields=['_id'])
            if u_count:
                for u_r in u_results():
                    direct_uid = u_r['_id']
        retweeted_uid = None
        if r['retweeted_status']:
            r_count, r_results = statuses_search.search(query={'_id': r['retweeted_status']}, fields=['user'])
            if r_count:
                for r_r in r_results():
                    retweeted_uid = r_r['user']

        for i in xrange(-total_days + 1, 1):
            gt = now_ts + during * (i - 1)
            lt = now_ts + during * i
            if gt < r['timestamp'] <= lt:
                for from_uid in [direct_uid, retweeted_uid]:
                    if from_uid:
                        try:
                            p_count = daily_interact_count_kv[str(uid) + '_' + str(from_uid) + '_' + str(lt)]
                            daily_interact_count_kv[str(uid) + '_' + str(from_uid) + '_' + str(lt)] = str(int(p_count) + 1)
                        except KeyError,e:
                            daily_interact_count_kv[str(uid) + '_' + str(from_uid) + '_' + str(lt)] = str(1)

        count += 1
        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts)
            ts = te
    
    batch = leveldb.WriteBatch()
    for k, v in daily_interact_count_kv.iteritems():
        batch.Put(k, str(v))
    user_daily_interact_count_bucket.Write(batch, sync=True)

if __name__ == '__main__':
    initial_load_user_interact_count()


