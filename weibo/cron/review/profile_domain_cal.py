# -*- coding: utf-8 -*-

import os
import sys
import time
import json
import heapq
import shutil
import leveldb
from config import LEVELDBPATH, DOMAIN_LIST


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


def calc_domain_airo():
    # test 0.5 seconds per 10000 users, total users 20000000
    count = 0
    ts = te = time.time()
    for k, v in daily_profile_counts_db.RangeIter():
        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts), ' %s daily calc_domain_airo' % now_datestr
            ts = te

        uid = str(k)
        active, important, reposts, original, emoticon = v.split('_\/')
        active = int(active)
        important = int(important)
        reposts = int(reposts)
        original = int(original)

        domain = user2domain(uid)

        try:
            _active, _important, _reposts, _original = daily_profile_domain_airo_bucket.Get(str(domain)).split('_\/')
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

        count += 1


def calc_domain_keywords():
    # test 2 seconds per 10000 users, total users 20000000
    count = 0
    ts = te = time.time()
    for k, v in daily_profile_keywords_db.RangeIter():
        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts), ' %s daily calc_domain_keywords' % now_datestr
            ts = te

        uid = str(k)
        domain = user2domain(uid)

        try:
            keywords_dict = json.loads(v)
        except Exception, e:
            print e
            count += 1
            continue
            
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
        print '-----', domainid, ' batch sort domain keywords'

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


def copytree(src, dst, symlinks=False, ignore=None):
    for item in os.listdir(src):
        s = os.path.join(src, item)
        d = os.path.join(dst, item)
        if os.path.isdir(s):
            shutil.copytree(s, d, symlinks, ignore)
        else:
            shutil.copy2(s, d)


if __name__ == '__main__':
    now_datestr = sys.argv[1]
    daily_profile_keywords_db = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_person_keywords_%s' % now_datestr),
                                                block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    daily_profile_counts_db = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_person_counts_%s' % now_datestr),
                                              block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

    try:
        os.mkdir(os.path.join(LEVELDBPATH, 'linhao_user2domain_profile_%s' % now_datestr))
    except:
        pass
    copytree(os.path.join(LEVELDBPATH, 'linhao_user2domain_profile'), \
             os.path.join(LEVELDBPATH, 'linhao_user2domain_profile_%s' % now_datestr))
    domain_leveldb = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_user2domain_profile_%s' % now_datestr),
                                     block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    
    try:
        shutil.rmtree(os.path.join(LEVELDBPATH, 'linhao_profile_domain_%s' % now_datestr))
    except:
        pass
    
    daily_profile_domain_airo_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_domain_%s' % now_datestr),
                                                       block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    
    daily_profile_domain_keywords_bucket = {}
    for i in range(9, 21):
        try:
            shutil.rmtree(os.path.join(LEVELDBPATH, './domain_keywords/linhao_profile_domain_keywords_%s_%s' % (now_datestr, i)))
        except:
            pass
        daily_profile_domain_keywords_bucket[i] = leveldb.LevelDB(os.path.join(LEVELDBPATH, './domain_keywords/linhao_profile_domain_keywords_%s_%s' % (now_datestr, i)),
                                                                  block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    calc_domain_airo()
    calc_domain_keywords()

    shutil.rmtree(os.path.join(LEVELDBPATH, 'linhao_user2domain_profile_%s' % now_datestr))
    
    # sort domain keywords, get topk
    daily_profile_domain_topk_keywords_bucket = {}
    for i in range(9, 21):
        daily_profile_domain_topk_keywords_bucket[i] = leveldb.LevelDB(os.path.join(LEVELDBPATH, './domain_keywords/linhao_profile_domain_topk_keywords_%s_%s' % (now_datestr, i)),
                                                                       block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    batch_sort_domain_keywords()
