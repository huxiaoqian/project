# -*- coding: utf-8 -*-

import os
import redis
import time
import leveldb
import heapq
import datetime
from model import WholeIdentification, AreaIdentification, BurstIdentification
from config import REDIS_HOST, REDIS_PORT, LEVELDBPATH, db, fields_value, labels

TOPK = 1000
DOMAIN_LIST = fields_value + labels
USER_DOMAIN = "user_domain" # user domain hash,
GLOBAL_ACTIVE_COUNT = "global_active_%s" # date as '20131227',
GLOBAL_IMPORTANT_COUNT = "global_important_%s" # date as '20131227',
user_followers_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_daily_user_followers'),
                                        block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

def get_daily_user_count_db_by_date(datestr):
    # datestr '20140105'
    daily_user_count_db = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'yuanshi_daily_count_%s' % datestr),
                                          block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    return daily_user_count_db


def _default_redis(host=REDIS_HOST, port=REDIS_PORT, db=0):
    return redis.StrictRedis(host, port, db)


def get_now_datestr():
    return datetime.datetime.now().strftime("%Y%m%d")


def get_before_datestr():
    now_ts = time.time() - 24 * 3600
    datestr = datetime.date.fromtimestamp(now_ts).isoformat().replace('-', '')
    return datestr


def clean_before_data():
    before_datestr = get_before_datestr()
    global_r0.delete(GLOBAL_ACTIVE_COUNT % before_datestr)
    global_r0.delete(GLOBAL_IMPORTANT_COUNT % before_datestr)


def getUserActive(uid):
    active = global_r0.hget(GLOBAL_ACTIVE_COUNT % now_datestr, str(uid))
    if not active:
        return 0
    
    return int(active)


def getUserImportant(uid):
    important = global_r0.hget(GLOBAL_IMPORTANT_COUNT % now_datestr, str(uid))
    if not important:
        return 0
    
    return int(important)


def getUserFollowerscount(uid):
    try:
        followers_count = int(user_followers_bucket.Get(str(uid)))
    except KeyError:
        followers_count = 0
 
    return followers_count


def user2domain(uid):
    domainid = global_r0.hget(USER_DOMAIN, str(uid))
    if not domainid:
        domainid = -1 # not taged label
    
    return int(domainid)


def redis2leveldb(batch_scan_count=10000):
    next_cursor, result_dicts = global_r0.hscan(GLOBAL_ACTIVE_COUNT % now_datestr, cursor=0, count=batch_scan_count)
    count = 0
    ts = te = time.time()
    uidset = set()
    while next_cursor != '0':
        batch = leveldb.WriteBatch()
        for uid, active in result_dicts.iteritems():
            followers_count = getUserFollowerscount(uid)
            important = getUserImportant(uid)
            domain = user2domain(uid)
            key = str(uid)
            value = str(active) + '_' + str(important) + '_' + str(followers_count) + '_' + str(domain)
            batch.Put(key, value)
            uidset.add(key)

        global_leveldb.Write(batch, sync=True)
        next_cursor, result_dicts = global_r0.hscan(GLOBAL_ACTIVE_COUNT % now_datestr, cursor=next_cursor, count=batch_scan_count)
        
        count += batch_scan_count
        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts)
            ts = te

class TopkHeap(object):
    def __init__(self, k):
        self.k = k
        self.data = []
 
    def Push(self, elem):
        if len(self.data) < self.k:
            heapq.heappush(self.data, elem)
        else:
            topk_small = self.data[0][0]
            if elem[0] > topk_small:
                heapq.heapreplace(self.data, elem)
 
    def TopK(self):
        return [x for x in reversed([heapq.heappop(self.data) for x in xrange(len(self.data))])]


def whole_domain_rank(topk=TOPK, identifyWindow=1):
    # 全网 领域 突发排序
    print 'whole domain burst rank'
    previous_exist = True
    if void_leveldb(global_leveldb):
        return

    if void_leveldb(global_previous_leveldb):
        previous_exist = False

    active_th = TopkHeap(topk)
    important_th = TopkHeap(topk)
    follower_th = TopkHeap(topk)

    domain_active_th = {}
    domain_important_th = {}
    domain_follower_th = {}

    domain_keys = set()
    for idx, domain in enumerate(DOMAIN_LIST):
        domainid = idx
        domain_active_th[domainid] = TopkHeap(topk)
        domain_important_th[domainid] = TopkHeap(topk)
        domain_follower_th[domainid] = TopkHeap(topk)
        domain_keys.add(domainid)

    burst_active_th = TopkHeap(topk)
    burst_important_th = TopkHeap(topk)

    count = 0
    ts = te = time.time() 
    for key, value in global_leveldb.RangeIter():
        active, important, follower, domain = value.split('_')
        uid = int(key)
        active = int(active)
        important = int(important)
        follower = int(follower)
        domain = int(domain)

        # 全网排序
        active_th.Push((active, important, follower, uid))
        important_th.Push((important, active, follower, uid))
        follower_th.Push((follower, active, important, uid))

        # 领域排序
        if domain in domain_keys:
            domain_active_th[domain].Push((active, important, follower, uid))
            domain_important_th[domain].Push((important, active, follower, uid))
            domain_follower_th[domain].Push((follower, active, important, uid))
        
        # 突发排序
        if previous_exist:
            try:
                value = global_previous_leveldb.Get(str(key))
                previous_active, previous_important, previous_follower, previous_domain = value.split('_')
            except KeyError:
                previous_active = previous_important = 0
            
            diff_active = active - int(previous_active)
            diff_important = important - int(previous_important)
            burst_active_th.Push((diff_active, diff_important, active, important, follower, uid))
            burst_important_th.Push((diff_important, diff_active, active, important, follower, uid))

        if count % 10000 == 0:
            te = time.time()
            print 'iter rank ', count, '%s sec' % (te - ts)
            ts = te
        count += 1

    save(active_th.TopK(), 'active', 'whole', identifyWindow)
    save(important_th.TopK(), 'important', 'whole', identifyWindow)
    save(follower_th.TopK(), 'followers', 'whole', identifyWindow)

    for idx, domain in enumerate(DOMAIN_LIST):
        domainid = idx
        save(domain_active_th[domainid].TopK(), 'active', 'domain', identifyWindow, domainid)
        save(domain_important_th[domainid].TopK(), 'important', 'domain', identifyWindow, domainid)
        save(domain_follower_th[domainid].TopK(), 'followers', 'domain', identifyWindow, domainid)
    
    save_burst(burst_active_th.TopK(), 'active')
    save_burst(burst_important_th.TopK(), 'important')


def save_burst(data, method="active", identifyWindow=1):
    for i, tuples in enumerate(data):
        rank = i + 1
        if method == 'active':
            diff_active, diff_important, active, important, follower, uid = tuples
        elif method == 'important':
            diff_important, diff_active, active, important, follower, uid = tuples

        print 'burst save: ', rank, diff_active, diff_important, follower, active, important, now_datestr, identifyWindow, method
        new_item = BurstIdentification(rank, uid, follower, active, important, diff_active, diff_important, now_datestr, identifyWindow, method)
        db.session.add(new_item)
        db.session.commit()


def save(data, method='active', module='whole', identifyWindow=1, domain=None):
    for i, tuples in enumerate(data):
        rank = i + 1
        if method == 'active':
            active, important, follower, uid = tuples
        elif method == 'important':
            important, active, follower, uid = tuples
        elif method == 'followers':
            follower, active, important, uid = tuples
        if module == 'whole':
            saveWhole2mysql(uid, active, important, follower, rank, identifyWindow, method)
        elif module == 'domain':
            saveDomain2mysql(uid, active, important, follower, domain, rank, identifyWindow, method)


def saveWhole2mysql(uid, active, important, followers, rank, identifyWindow, identifyMethod):
    print 'whole save: ', rank, uid, followers, active, important, now_datestr, identifyWindow, identifyMethod
    new_item = WholeIdentification(rank, uid, followers, active, important, now_datestr, identifyWindow, identifyMethod)
    db.session.add(new_item)
    db.session.commit()


def saveDomain2mysql(uid, active, important, followers, domain, rank, identifyWindow, identifyMethod):
    print 'domain save: ', domain, rank, uid, followers, active, important, now_datestr, identifyWindow, identifyMethod
    new_item = AreaIdentification(domain, rank, uid, followers, active, important, now_datestr, identifyWindow, identifyMethod)
    db.session.add(new_item)
    db.session.commit()


def test_read():
    count = 0
    for k, v in global_leveldb.RangeIter():
        count += 1
    print count


def void_leveldb(ldb):
    count = 0
    for k, v in ldb.RangeIter():
        count += 1
    
    if count > 0:
        return False
    else:
        return True
        

if __name__ == '__main__':
    before_datestr = get_before_datestr()
    now_datestr = get_now_datestr()
    global_r0 = _default_redis()
    global_previous_leveldb = get_daily_user_count_db_by_date(before_datestr)
    global_leveldb = get_daily_user_count_db_by_date(now_datestr)

    # save to leveldb
    redis2leveldb()
    clean_before_data()
    test_read()

    # save to mysql
    from time_utils import ts2datetime, datetimestr2ts
    now_datestr = ts2datetime(datetimestr2ts(now_datestr))
    whole_domain_rank()
