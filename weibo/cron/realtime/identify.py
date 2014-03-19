# -*- coding: utf-8 -*-

import os
import re
import sys
import time
import json
import redis
import heapq
import shutil
import leveldb
import datetime
from dynamic_xapian_weibo import getXapianWeiboByDate
from model import WholeIdentification, AreaIdentification
from config import LEVELDBPATH, DOMAIN_LIST, xapian_search_user, \
                   db, DYNAMIC_XAPIAN_WEIBO_STUB_PATH

TOPK = 1000


def calc_ai(active, important, reposts_count):
    # 更新该条微博发布者的重要度、活跃度
    active += 1
    important += reposts_count
    return active, important


def calc_retweeted_important(retweeted_uid, text):
    # 更新直接转发或原创用户的重要度 + 1，活跃度不变
    if retweeted_uid != 0:
        # 该条微博为转发微博
        repost_user = re.search('//@([a-zA-Z-_\u0391-\uFFE5]+)', text)
        if repost_user:
            direct_uid = username2uid(repost_user)

            if direct_uid:
                retweeted_uid = direct_uid

        return retweeted_uid
    else:
        # 该条微博为原创微博
        return None


def get_aifd(uid):
    try:
        active, important, follower, domain = daily_identify_aifd_bucket.Get(str(uid)).split('_')
        active = int(active)
        important = int(important)
        follower = int(follower)
    except KeyError:
        active = 0
        important = 0
        follower = 0
        domain = 20
    return active, important, follower, domain


def username2uid(name):
    # 从leveldb根据用户昵称检索用户uid
    uid = None
    try:
        uid = username_leveldb.Get(str(name))
    except KeyError:
        pass
    return uid


def ai_xapian2leveldb():
    # test 0.8 seconds per 10000 weibos, 100000000 weibos, total time 2.2 h
    weibos = xapian_search_weibo.iter_all_docs(fields=['user', 'retweeted_uid', 'reposts_count', 'text'])
    count = 0
    ts = te = time.time()
    for weibo in weibos:
        if count % 10000 == 0:
            te = time.time()
            print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())), count, '%s sec' % (te - ts), 'identify ai xapian to leveldb', now_datestr
            ts = te
        count += 1

        #
        uid = weibo['user']
        reposts_count = weibo['reposts_count']
        retweeted_uid = weibo['retweeted_uid']
        text = weibo['text']

        #
        active, important, follower, domain = get_aifd(uid)
        active, important = calc_ai(active, important, reposts_count)
        
        #
        value = '_'.join([str(active), str(important), str(follower), str(domain)])
        daily_identify_aifd_bucket.Put(str(uid), value)

        # 更新直接转发或原创用户的重要度 + 1，活跃度不变
        retweeted_uid = calc_retweeted_important(retweeted_uid, text)
        if retweeted_uid:
            active, important, follower, domain = get_aifd(retweeted_uid)
            important += 1

            value = '_'.join([str(active), str(important), str(follower), str(domain)])
            daily_identify_aifd_bucket.Put(str(retweeted_uid), value)


def update_follower2leveldb():
    # 从leveldb更新leveldb的用户粉丝数数据
    # test 0.15 seconds per 10000 users, total 22670000 users, 0.1 h
    users = xapian_search_user.iter_all_docs(fields=['user', 'followers_count'])
    
    count = 0
    ts = te = time.time()
    for k, v in user_followers_count_leveldb.RangeIter():
        uid = int(k)
        follower = int(v)
        
        try:
            active, important, _follower, domain = daily_identify_aifd_bucket.Get(str(uid)).split('_')
        except KeyError:
            active = 0
            important = 0
            domain = 20

        daily_identify_aifd_bucket.Put(str(uid), str(active) + '_' + str(important) + '_' + \
                                       str(follower) + '_' + str(domain))

        if count % 10000 == 0:
            te = time.time()
            print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())), count, '%s sec' % (te - ts), ' identify person follower', now_datestr
            ts = te
        count += 1


def update_domain2leveldb():
    # 从leveldb更新leveldb的用户领域所属数据
    # test 0.15 seconds per 10000 users, total 22670000 users, 0.09 h
    count = 0
    ts = te = time.time()
    for k, v in domain_leveldb.RangeIter():
        uid, datestr = k.split('_')
        uid = int(uid)
        domainid = DOMAIN_LIST.index(v)

        try:
            active, important, follower, _domain = daily_identify_aifd_bucket.Get(str(uid)).split('_')
        except KeyError:
            active = 0
            important = 0
            follower = 0
        
        domain = domainid
        daily_identify_aifd_bucket.Put(str(uid), str(active) + '_' + str(important) + '_' + \
                                       str(follower) + '_' + str(domain))

        if count % 10000 == 0:
            te = time.time()
            print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())), count, '%s sec' % (te - ts), ' identify person domain', now_datestr
            ts = te
        count += 1


def copytree(src, dst, symlinks=False, ignore=None):
    for item in os.listdir(src):
        s = os.path.join(src, item)
        d = os.path.join(dst, item)
        if os.path.isdir(s):
            shutil.copytree(s, d, symlinks, ignore)
        else:
            shutil.copy2(s, d)


def get_now_datestr():
    return datetime.datetime.now().strftime("%Y%m%d")


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


def void_leveldb(ldb):
    count = 0
    for k, v in ldb.RangeIter():
        count += 1
    
    if count > 0:
        return False
    else:
        return True


def whole_domain_rank(topk=TOPK, identifyWindow=1):
    # 全网 领域 排序
    # test 30000000 users, about 30 minutes
    print '%s whole domain rank' % time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    if void_leveldb(global_leveldb):
        return

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

    count = 0
    ts = te = time.time() 
    for key, value in global_leveldb.RangeIter():
        active, important, follower, domain = value.split('_')
        try:
            uid = int(key)
        except:
            count += 1
            continue
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

        if count % 10000 == 0:
            te = time.time()
            print '%s iter rank ' % time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())), count, '%s sec' % (te - ts), now_datestr
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


def copytree(src, dst, symlinks=False, ignore=None):
    for item in os.listdir(src):
        s = os.path.join(src, item)
        d = os.path.join(dst, item)
        if os.path.isdir(s):
            shutil.copytree(s, d, symlinks, ignore)
        else:
            shutil.copy2(s, d)


def save(data, method='active', module='whole', identifyWindow=1, domain=None):
    # delete old data
    if module == 'whole':
        if data and len(data):
            exist_items = db.session.query(WholeIdentification).\
                                    filter(WholeIdentification.identifyDate == now_datestr, \
                                           WholeIdentification.identifyWindow==identifyWindow, \
                                           WholeIdentification.identifyMethod==method).all()
            for exist_item in exist_items:
                db.session.delete(exist_item)
            db.session.commit()

    elif module == 'domain':
        if data and len(data):
            exist_items = db.session.query(AreaIdentification).\
                                    filter(AreaIdentification.identifyDate == now_datestr, \
                                           AreaIdentification.identifyWindow==identifyWindow, \
                                           AreaIdentification.identifyMethod==method, \
                                           AreaIdentification.topicId==domain).all()
            for exist_item in exist_items:
                db.session.delete(exist_item)
            db.session.commit()

    # add new data
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
    print '%s whole save: ' % time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))#, rank, uid, followers, active, important, now_datestr, identifyWindow, identifyMethod
    new_item = WholeIdentification(rank, uid, followers, active, important, now_datestr, identifyWindow, identifyMethod)
    db.session.add(new_item)
    db.session.commit()


def saveDomain2mysql(uid, active, important, followers, domain, rank, identifyWindow, identifyMethod):
    print '%s domain save: ' % time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))#, domain, rank, uid, followers, active, important, now_datestr, identifyWindow, identifyMethod
    new_item = AreaIdentification(domain, rank, uid, followers, active, important, now_datestr, identifyWindow, identifyMethod)
    db.session.add(new_item)
    db.session.commit()


if __name__ == '__main__':
    print "%s start" % time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    
    # get now_datestr
    now_datestr = sys.argv[1] # datestr as '20130921'
    daily_identify_aifd_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'yuanshi_daily_count_%s' % now_datestr),
                                                 block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

    # check if xapian data is ready
    xapian_stub_file = '%s%s' % (DYNAMIC_XAPIAN_WEIBO_STUB_PATH, now_datestr)
    while 1:
        if os.path.isfile(xapian_stub_file):
            print '%s xapian data stub file is prepared' % now_datestr
            break
        else:
            print '%s xapian data stub file is not prepared' % now_datestr
            time.sleep(60)

    # init xapian weibo
    xapian_search_weibo = getXapianWeiboByDate(now_datestr)

    # init leveldb
    try:
        shutil.rmtree(os.path.join(LEVELDBPATH, 'yuanshi_daily_count_%s' % now_datestr))
    except:
        pass
    daily_identify_aifd_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'yuanshi_daily_count_%s' % now_datestr),
                                                 block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

    try:
        os.mkdir(os.path.join(LEVELDBPATH, 'linhao_user2followers_identify_r_%s' % now_datestr))
    except:
        pass
    copytree(os.path.join(LEVELDBPATH, 'yuanshi_daily_user_followers'), \
             os.path.join(LEVELDBPATH, 'linhao_user2followers_identify_r_%s' % now_datestr))
    user_followers_count_leveldb = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_user2followers_identify_r_%s' % now_datestr),
                                                   block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    
    try:
        os.mkdir(os.path.join(LEVELDBPATH, 'linhao_user2domain_identify_r_%s' % now_datestr))
    except:
        pass
    copytree(os.path.join(LEVELDBPATH, 'linhao_user2domain_identify'), \
             os.path.join(LEVELDBPATH, 'linhao_user2domain_identify_r_%s' % now_datestr))
    domain_leveldb = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_user2domain_identify_r_%s' % now_datestr),
                                                  block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    
    try:
        os.mkdir(os.path.join(LEVELDBPATH, 'linhao_user_name_identify_r_%s' % now_datestr))
    except:
        pass
    copytree(os.path.join(LEVELDBPATH, 'linhao_user_name_identify'), \
             os.path.join(LEVELDBPATH, 'linhao_user_name_identify_r_%s' % now_datestr))
    username_leveldb = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_user_name_identify_r_%s' % now_datestr),
                                                    block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    
    # calculate
    ai_xapian2leveldb()
    update_follower2leveldb()
    update_domain2leveldb()

    # remove leveldb
    shutil.rmtree(os.path.join(LEVELDBPATH, 'linhao_user2followers_identify_r_%s' % now_datestr))
    shutil.rmtree(os.path.join(LEVELDBPATH, 'linhao_user2domain_identify_r_%s' % now_datestr))
    shutil.rmtree(os.path.join(LEVELDBPATH, 'linhao_user_name_identify_r_%s' % now_datestr))

    # identify rank
    global_leveldb = daily_identify_aifd_bucket
    #whole_domain_rank()

    # identify burst rank
    from identify_burst import get_before_datestr, burst_rank
    before_datestr = get_before_datestr(now_datestr) # '20130830'

    global_leveldb = daily_identify_aifd_bucket
    global_previous_leveldb = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'yuanshi_daily_count_%s' % before_datestr),
                                              block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    burst_rank(now_datestr, before_datestr, global_leveldb, global_previous_leveldb)

    print "%s end" % time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
