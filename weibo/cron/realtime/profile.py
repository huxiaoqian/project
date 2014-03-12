# -*- coding: utf-8 -*-

import os
import sys
import time
import json
import heapq
import leveldb
import datetime
from config import LEVELDBPATH, DOMAIN_LIST
from dynamic_xapian_weibo import getXapianWeiboByDate


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


def user2domain(uid, updatetime='20131220'):
    try:
        v = domain_leveldb.Get(str(uid) + '_' + str(updatetime))
        domainid = DOMAIN_LIST.index(v)
    except KeyError:
        domainid = 20

    return domainid


def calc_domain_airo_keywords():
    # test 2 seconds per 10000 users, total users 20000000
    count = 0
    ts = te = time.time()
    for k, v in daily_profile_airoeik_bucket.RangeIter():
        if count % 10000 == 0:
            te = time.time()
            print  time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())), count, '%s sec' % (te - ts), ' %s daily calc_domain_airo_keywords' % now_datestr
            ts = te

        uid = str(k)
        active, important, reposts, original, emoticon, interact_dict, keywords_dict = v.split('_\/')
        active = int(active)
        important = int(important)
        reposts = int(reposts)
        original = int(original)

        domain = user2domain(uid)

        try:
            _active, _important, _reposts, _original = daily_profile_domain_airo_bucket.Get(uid).split('_\/')
            _active = int(_active)
            _important = int(_important)
            _reposts = int(_reposts)
            _original = int(_original)
        except KeyError:
            _active = 0
            _important = 0
            _reposts = 0
            _original = 0

        _active += active
        _important += important
        _reposts += reposts
        _original += original
        
        key = str(domain)
        value = '_\/'.join([str(_active), str(_important), str(_reposts), str(_original)])
        daily_profile_domain_airo_bucket.Put(key, value)

        keywords_dict = json.loads(keywords_dict)
        keywords_leveldb = daily_profile_domain_keywords_bucket[int(domain)]
        for k, v in keywords_dict.iteritems():
            try:
                kcount = int(keywords_leveldb.Get(str(k.encode('utf-8'))))
                kcount += int(v)
            except KeyError:
                kcount = int(v)

            keywords_leveldb.Put(str(k.encode('utf-8')), str(kcount))

        count += 1


def batch_sort_domain_keywords(topk=50):    
    for domainid in range(9, 21):
        print '%s-----' % time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())), domainid, ' batch sort domain keywords'

        keywords_th = TopkHeap(topk)
        db = daily_profile_domain_keywords_bucket[domainid]

        for k, v in db.RangeIter():
            v = int(v)
            keywords_th.Push((v, k))

        top_keywords = keywords_th.TopK()
        top_keywords_dict = {}

        for count, keywords in top_keywords:
            top_keywords_dict[keywords] = count

        daily_profile_domain_topk_keywords_bucket[domainid].Put(str(domainid), json.dumps(top_keywords_dict))


def get_now_datestr():
    return datetime.datetime.now().strftime("%Y%m%d")


def get_before_datestr():
    now_ts = time.time() - 24 * 3600
    datestr = datetime.date.fromtimestamp(now_ts).isoformat().replace('-', '')
    return datestr


if __name__ == '__main__':

    print "%s start" % time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
	# get datestr
    now_datestr = '20130901' #get_now_datestr()
    
	# init xapian weibo
    xapian_search_weibo = getXapianWeiboByDate(now_datestr)

    # init leveldb
    daily_profile_airoeik_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_person_%s' % now_datestr),
                                                   block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    domain_leveldb = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_user2domain_profile'),
                                                  block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    daily_profile_domain_airo_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_domain_%s' % now_datestr),
                                                       block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    daily_profile_domain_keywords_bucket = {}
    for i in range(9, 21):
        daily_profile_domain_keywords_bucket[i] = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_domain_keywords_%s_%s' % (now_datestr, i)),
                                                                  block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

    # calculate
    calc_domain_airo_keywords()

    # sort domain keywords, get topk
    daily_profile_domain_topk_keywords_bucket = {}
    for i in range(9, 21):
        daily_profile_domain_topk_keywords_bucket[i] = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_domain_topk_keywords_%s_%s' % (now_datestr, i)),
                                                                       block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    batch_sort_domain_keywords()

    print "%s end" % time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
