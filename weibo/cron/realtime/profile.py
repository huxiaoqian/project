# -*- coding: utf-8 -*-

import re
import os
import sys
import time
import json
import shutil
import heapq
import leveldb
import datetime
from dynamic_xapian_weibo import getXapianWeiboByDate
from config import LEVELDBPATH, DOMAIN_LIST, DYNAMIC_XAPIAN_WEIBO_STUB_PATH


# init seed_set
seed_set = set([])
with open('official_emoticons.txt') as f:
    for l in f:
        seed_set.add(l.rstrip())


def emoticon_find(text):
    emotion_pattern = r'\[(\S+?)\]'
    remotions = re.findall(emotion_pattern, text)

    emoticons = []
    haveEmoticon = False
    if remotions:
        for e in remotions:
            if e in seed_set:
                haveEmoticon = True
                break
    
    return haveEmoticon


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


def get_airoe(uid):
    try:
        active, important, reposts, original, emoticon = daily_profile_airoe_bucket.Get(str(uid)).split('_\/')
        active = int(active)
        important = int(important)
        reposts = int(reposts)
        original = int(original)
        emoticon = int(emoticon)
    except:
        active = important = reposts = original = emoticon = 0

    return active, important, reposts, original, emoticon


def calc_ai(active, important, reposts_count):
    # 更新该条微博发布者的重要度、活跃度
    active += 1
    important += reposts_count
    return active, important


def username2uid(name):
    # 从leveldb根据用户昵称检索用户uid
    uid = None
    try:
        uid = username_leveldb.Get(str(name))
    except KeyError:
        pass
    return uid


def calc_retweeted_important(retweeted_uid, text):
    # 更新直接转发或原创用户的重要度 + 1，活跃度不变
    if retweeted_uid != 0:
        # 该条微博为转发微博
        repost_user = re.search('//@[a-zA-Z-_\u0391-\uFFE5]+', text)
        if repost_user:
            direct_uid = username2uid(repost_user)

            if direct_uid:
                retweeted_uid = direct_uid

            return retweeted_uid
        else:
            # 该条微博为原创微博
            return None


def calc_person_airoei_xapian2leveldb():
    weibos = xapian_search_weibo.iter_all_docs(fields=['user', 'retweeted_mid', 'retweeted_uid', 'reposts_count', \
                                                       'comments_count', 'text'])
    count = 0
    ts = te = time.time()
    for weibo in weibos:
        if count % 10000 == 0:
            te = time.time()
            print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())), count, '%s sec' % (te - ts), 'calc_person_airoei_xapian2leveldb', now_datestr
            ts = te
        count += 1

        #
        uid = weibo['user']
        retweeted_mid = weibo['retweeted_mid']
        retweeted_uid = weibo['retweeted_uid']
        reposts_count = weibo['reposts_count']
        comments_count = weibo['comments_count']
        text = weibo['text']

        # get
        active, important, reposts, original, emoticon = get_airoe(uid)
        try:
            interact_dict = json.loads(daily_profile_interact_bucket.Get(str(uid)))
            direct_interact = interact_dict['direct']
            retweeted_interact = interact_dict['retweeted']
        except:
            direct_interact = {}
            retweeted_interact = {}

        #calc
        active, important = calc_ai(active, important, reposts_count)
        
        if retweeted_mid != 0:
            reposts += 1
        else:
            original += 1

        _emoticons = emoticon_find(text)
        if _emoticons:
            emoticon += 1

        if isinstance(text, str):
            text = text.decode('utf-8', 'ignore')

        RE = re.compile(u'//@([a-zA-Z-_⺀-⺙⺛-⻳⼀-⿕々〇〡-〩〸-〺〻㐀-䶵一-鿃豈-鶴侮-頻並-龎]+):', re.UNICODE)
        repost_users = RE.findall(text)

        if len(repost_users):
            repost_user = repost_users[0]
            try:
                direct_interact[repost_user] += 1
            except KeyError:
                direct_interact[repost_user] = 1

        if retweeted_uid != 0:
            try:
                retweeted_interact[retweeted_uid] += 1
            except KeyError:
                retweeted_interact[retweeted_uid] = 1
        
        # save interact
        interact_dict = {'direct': direct_interact, 'retweeted': retweeted_interact}
        daily_profile_interact_bucket.Put(str(uid), json.dumps(interact_dict))

        # save
        value = '_\/'.join([str(active), str(important), str(reposts), str(original), str(emoticon)])
        daily_profile_airoe_bucket.Put(str(uid), value)

        # 更新直接转发或原创用户的重要度 + 1，活跃度不变
        retweeted_uid = calc_retweeted_important(retweeted_uid, text)
        if retweeted_uid:
            active, important, reposts, original, emoticon = get_airoe(retweeted_uid)
            important += 1

            value = '_\/'.join([str(active), str(important), str(reposts), str(original), str(emoticon)])
            daily_profile_airoe_bucket.Put(str(retweeted_uid), value)


def calc_domain_airo():
    # test 0.25 seconds per 10000 users, total users 20000000
    count = 0
    ts = te = time.time()
    for k, v in daily_profile_airoe_bucket.RangeIter():
        if count % 10000 == 0:
            te = time.time()
            print  time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())), count, '%s sec' % (te - ts), ' %s daily calc_domain_airo' % now_datestr
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
    # test 1.5 seconds per 10000 users, total users 20000000
    count = 0
    ts = te = time.time()
    for k, v in daily_profile_keywords_bucket.RangeIter():
        if count % 10000 == 0:
            te = time.time()
            print  time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())), count, '%s sec' % (te - ts), ' %s daily calc_domain_keywords' % now_datestr
            ts = te

        uid = str(k)
        keywords_dict = json.loads(v)

        domain = user2domain(uid)

        keywords_leveldb = daily_profile_domain_keywords_bucket[int(domain)]
        for _k, _v in keywords_dict.iteritems():
            try:
                kcount = int(keywords_leveldb.Get(str(_k.encode('utf-8'))))
                kcount += int(_v)
            except KeyError:
                kcount = int(_v)

            keywords_leveldb.Put(str(_k.encode('utf-8')), str(kcount))

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


def copytree(src, dst, symlinks=False, ignore=None):
    for item in os.listdir(src):
        s = os.path.join(src, item)
        d = os.path.join(dst, item)
        if os.path.isdir(s):
            shutil.copytree(s, d, symlinks, ignore)
        else:
            shutil.copy2(s, d)


if __name__ == '__main__':
    print "%s start" % time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
	
    # get datestr
    now_datestr = sys.argv[1] # datestr as '20130922'

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
    daily_profile_airoe_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_person_counts_%s' % now_datestr),
                                                 block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    daily_profile_interact_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_person_interact_%s' % now_datestr),
                                                    block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    '''
    try:
        os.mkdir(os.path.join(LEVELDBPATH, 'linhao_user_name_profile_%s' % now_datestr))
    except:
        pass
    copytree(os.path.join(LEVELDBPATH, 'linhao_user_name_profile'), \
             os.path.join(LEVELDBPATH, 'linhao_user_name_profile_%s' % now_datestr))
    username_leveldb = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_user_name_profile_%s' % now_datestr),
                                       block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

    # calculate profile person counts and interact
    calc_person_airoei_xapian2leveldb()
    
    # remove leveldb
    shutil.rmtree(os.path.join(LEVELDBPATH, 'linhao_user_name_profile_%s' % now_datestr))
    '''
    # init leveldb
    daily_profile_keywords_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_person_keywords_%s' % now_datestr),
                                                    block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    domain_leveldb = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_user2domain_profile'),
                                     block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    daily_profile_domain_airo_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_domain_%s' % now_datestr),
                                                       block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    daily_profile_domain_keywords_bucket = {}
    for i in range(9, 21):
        daily_profile_domain_keywords_bucket[i] = leveldb.LevelDB(os.path.join(LEVELDBPATH, './domain_keywords/linhao_profile_domain_keywords_%s_%s' % (now_datestr, i)),
                                                                  block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

    # calculate
    # calc_domain_airo()
    calc_domain_keywords()

    # sort domain keywords, get topk
    daily_profile_domain_topk_keywords_bucket = {}
    for i in range(9, 21):
        daily_profile_domain_topk_keywords_bucket[i] = leveldb.LevelDB(os.path.join(LEVELDBPATH, './domain_keywords/linhao_profile_domain_topk_keywords_%s_%s' % (now_datestr, i)),
                                                                       block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    batch_sort_domain_keywords()

    print "%s end" % time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
