# -*- coding: utf-8 -*-
import time, json, os
from xapian_weibo.xapian_backend import XapianSearch
from view import app
import leveldb, redis

app = app.test_client()
xapian_search_weibo = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_weibo', schema_version=2)
xapian_search_user = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_user', schema_version=1)
LEVELDBPATH = '/home/mirage/leveldb'
buckets = {}

redis_host = 'localhost'
redis_port = 6379
redis_conn = redis.Redis(redis_host, redis_port)


def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print '%r %2.4f sec' % (method.__name__, te-ts)
        return result
    return timed


def get_bucket(bucket):
    if bucket in buckets:
        return buckets[bucket]
    buckets[bucket] = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_' + bucket), block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))    
    return buckets[bucket]

friendship_bucket = get_bucket('friendship')

def date2ts(datestr):
    return int(time.mktime(time.strptime(datestr, '%Y-%m-%d  %H:%M:%S')))

@timeit
def getUserPostCount(uid):
    time_arr = []
    post_arr = []
    repost_arr = []
    fipost_arr = []
    during = 24 * 3600
    begints = date2ts('2013-01-01 02:00:00')
    endts = date2ts('2013-05-01 02:00:00')

    total_days = (endts - begints) / during

    for i in range(0, total_days):
        end_ts = endts - i * during
        begin_ts = end_ts - during 

        fipost_query_dict = {
            'timestamp': {
                '$gt': begin_ts,
                '$lt': end_ts
            },
            'user': int(uid),
            'retweeted_status': None
        }

        repost_query_dict = {
            'timestamp': {
                '$gt': begin_ts,
                '$lt': end_ts
            },
            'user': int(uid),
            '$not': {'retweeted_status': None}
        }

        fipost_count = xapian_search_weibo.search(query=fipost_query_dict, count_only=True)
        repost_count = xapian_search_weibo.search(query=repost_query_dict, count_only=True)
        total_count = fipost_count + repost_count
        post_arr.append(total_count)
        fipost_arr.append(fipost_count)
        repost_arr.append(repost_count)
        time_arr.append(end_ts)
    print 'sum count: ', sum(post_arr)

    return json.dumps({'time': time_arr, 'count': post_arr, 'repost': repost_arr, 'fipost': fipost_arr})

@timeit
def getUserPostCountByIter(uid):
    time_arr = []
    post_arr = []
    repost_arr = []
    fipost_arr = []
    during = 24 * 3600
    begints = date2ts('2013-01-01 02:00:00')
    endts = date2ts('2013-05-01 02:00:00')

    total_days = (endts - begints) / during

    for i in range(0, total_days):
        end_ts = endts - i * during
        begin_ts = end_ts - during 

        query_dict = {
            'timestamp': {
                '$gt': begin_ts,
                '$lt': end_ts
            },
            'user': int(uid),
        }

        count, get_results = xapian_search_weibo.search(query=query_dict, fields=['retweeted_status'])
        post_count = 0
        fipost_count = 0
        repost_count = 0
        for r in get_results():
            if r['retweeted_status']:
                repost_count += 1
            else:
                fipost_count += 1
            post_count += 1

        post_arr.append(post_count)
        fipost_arr.append(fipost_count)
        repost_arr.append(repost_count)
        time_arr.append(end_ts)
    print 'sum count: ', sum(post_arr)

    return json.dumps({'time': time_arr, 'count': post_arr, 'repost': repost_arr, 'fipost': fipost_arr})

@timeit
def test_person_weibo_count(uid, days_num, m):
    rv = app.get('/profile/person_count/' + str(uid) + '?interval=' + str(days_num) + '&m=' + str(m)) 
    rv_data = json.loads(rv.data)
    print rv_data

@timeit
def test_person_basic_info(uid):
    rv = app.get('/profile/person/' + str(uid))
    rv_data = json.loads(rv.data)
    print rv_data

@timeit
def test_person_fri_fol_from_xapian():
    total_friends_count = 0
    for uid in [1182389073, 1182391231, 1187986757, 1193491727, 1195818302]:
        count, get_results = xapian_search_user.search(query={'_id': int(uid)}, fields=['friends'])
        for r in get_results():
            friends = r['friends']
            total_friends_count += len(friends)
    print 'total friends count: ', total_friends_count

@timeit
def test_person_fri_fol_from_leveldb():
    total_friends_count = 0
    for uid in [1182389073, 1182391231, 1187986757, 1193491727, 1195818302]:
        friends_key = str(uid) + '_' + 'friends'
        try:
            friends = json.loads(friendship_bucket.Get(friends_key))
        except KeyError:
            friends = []
        total_friends_count += len(friends)
    print 'total friends count: ', total_friends_count

@timeit
def test_write_fri_from_xapian_to_redis():
    count, get_results = xapian_search_user.search(query={'_id': {'$gt': 0}}, fields=['_id', 'friends'])
    count = 0
    hit_count = 0
    total_friends_count = 0
    pipe = redis_conn.pipeline()
    pipe.multi()
    for r in get_results():
        uid = r['_id']
        if count % 10000 == 0:
            print count
        count += 1
        if r['friends'] and len(r['friends']):
            friends = r['friends']
            key_prefix = 'linhao_friends_' + str(uid)
            pipe.spop(key_prefix)
            for friend in friends:
                pipe.sadd(key_prefix, friend)
            pipe.execute()
            hit_count += 1
            total_friends_count += len(friends)
            
    print 'hit count: ', hit_count
    print 'total_friends_count: ', total_friends_count

@timeit
def test_read_fri_fol_from_redis():
    total_friends_count = 0
    for uid in [1182389073, 1182391231, 1187986757, 1193491727, 1195818302]:
        key_prefix = 'linhao_friends_' + str(uid)
        friends = redis_conn.smembers(key_prefix)
        total_friends_count += len(friends)
    print 'total_friends_count: ', total_friends_count

@timeit
def test_write_fri_fol_from_xapian_to_leveldb():
    query_dict = {
        '_id': {
            '$gt': 0,
        }
    }
    count, get_results = xapian_search_user.search(query=query_dict, fields=['_id', 'friends', 'followers'])

    count = 0
    hit_count = 0
    total_friends_count = 0
    for r in get_results():
        uid = r['_id']
        friends = r['friends']
        if friends and len(friends):
            k = str(uid) + '_' + 'friends'
            v = json.dumps(friends)
            friendship_bucket.Put(k, str(v))
            hit_count += 1
            total_friends_count += len(friends)
    
    print 'hit count: ', hit_count
    print 'total_friends_count: ', total_friends_count    

def main():
    '''获取用户发布的微博数量
    '''
    # #hit count(fipost+repost, 120 days): 9858
    # #time: 5.7802s
    # getUserPostCount(1813080181)

    # #hit count(fipost+repost, 120 days): 9858
    # #time: 8.1234s
    # getUserPostCountByIter(1813080181)

    # #search range: 10亿条微博, hit count: 29129
    # #time: 15.991s
    # test_person_weibo_count(1813080181, 240, 'test')

    # #search range: 2.5亿条微博, hit count: 9649
    # #time: 3.5109s
    # test_person_weibo_count(1813080181, 240, 'true')

    # #time: 0.0021s
    # test_person_basic_info(1813080181)

    # #time: 0.0032s
    # #total friends count: 2254 
    # test_person_fri_fol_from_xapian()

    # #time: 0.0013s
    # #total friends count: 2254 
    # test_person_fri_fol_from_leveldb()

    # #user count: 681668, total friends count: 256290066
    # #time: 1123.0507s
    # test_write_fri_from_xapian_to_redis()
    
    # #time: 0.0146s
    # #total friends count: 2254
    # test_read_fri_fol_from_redis()

    # #user count: 681668, total friends count: 256290066
    # #time: 1146.0821s
    # test_write_fri_fol_from_xapian_to_leveldb()

    
if __name__ == '__main__':
    main()