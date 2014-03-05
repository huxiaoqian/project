# -*- coding: utf-8 -*-

import os
import re
import sys
import time
import json
import heapq
import shutil
import leveldb
import datetime
from config import LEVELDBPATH, DOMAIN_LIST
from xapian_weibo.utils import load_scws, cut
from dynamic_xapian_weibo import getXapianWeiboByDate


# init scws
scws = load_scws()

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


def _utf_encode(s):
    if isinstance(s, unicode):
        return s.encode('utf-8', 'ignore')
    else:
        return s


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


def get_airoeik(uid):
    try:
        active, important, reposts, original, emoticon, direct_interact, \
        retweeted_interact, keywords_dict = daily_profile_airoeik_bucket.Get(str(uid)).split('_\/')
        active = int(active)
        important = int(important)
        reposts = int(reposts)
        original = int(original)
        emoticon = int(emoticon)
    except KeyError:
        active = important = reposts = original = emoticon = 0
        direct_interact = ''
        retweeted_interact = ''
        keywords_dict = ''

    return active, important, reposts, original, emoticon, direct_interact, \
           retweeted_interact, keywords_dict


def calc_roeik(retweeted_mid, retweeted_uid, text, reposts, original, emoticon, direct_interact, retweeted_interact, keywords_dict):
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
        direct_interact = str(direct_interact.encode('utf-8')) + '\_/' + str(repost_user.encode('utf-8'))

    if retweeted_uid != 0:
        retweeted_interact = str(retweeted_interact) + '\_/' + str(retweeted_uid)
    
    '''
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
    '''

    interact_dict = {'direct': direct_interact, 'retweeted': retweeted_interact}

    terms = cut(scws, _utf_encode(text), f='n')
    keywords_dict += '\_/' + '\_/'.join(terms)
    '''
    for term in terms:
        try:
            keywords_dict[term] += 1
        except KeyError:
            keywords_dict[term] = 1
    '''

    return reposts, original, emoticon, interact_dict, keywords_dict


def calc_person_airoeik_xapian2leveldb():
    weibos = xapian_search_weibo.iter_all_docs(fields=['user', 'retweeted_uid', 'retweeted_mid', \
                                                       'reposts_count', 'text'])
    count = 0
    ts = te = time.time()
    for weibo in weibos:
        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts), ' profile airoeik xapian to leveldb', now_datestr
            ts = te
        count += 1

        #
        uid = weibo['user']
        reposts_count = weibo['reposts_count']
        retweeted_uid = weibo['retweeted_uid']
        retweeted_mid = weibo['retweeted_mid']
        text = weibo['text']

        #
        active, important, reposts, original, emoticon, direct_interact, \
        retweeted_interact, keywords_dict = get_airoeik(uid)

        #
        active += 1
        important += reposts_count

        #
        reposts, original, emoticon, interact_dict, keywords_dict = calc_roeik(retweeted_mid, \
        retweeted_uid, text, reposts, original, emoticon, direct_interact, retweeted_interact, keywords_dict)
        
        value = '_\/'.join([str(active), str(important), str(reposts), str(original), str(emoticon), direct_interact, retweeted_interact, keywords_dict])
        daily_profile_airoeik_bucket.Put(str(uid), str(value))


def calc_domain_airo_keywords():
    # test 2 seconds per 10000 users, total users 20000000
    count = 0
    ts = te = time.time()
    for k, v in daily_profile_airoeik_bucket.RangeIter():
        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts), ' %s daily calc_domain_airo_keywords' % now_datestr
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


def personTopic2leveldb(keyword_limit=50):
    # test 0.6 seconds per 10000 weibos
    weibos = xapian_search_weibo.iter_all_docs(fields=['user', 'text'])

    count = 0
    ts = te = time.time()
    for weibo in weibos:
        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts), ' profile person topic 2 leveldb ', now_datestr
            ts = te

        uid = weibo['user']
        
        text = _utf_encode(weibo['text'])
        terms = cut(scws, text, f='n')

        try:
            ori_dict = json.loads(daily_profile_person_topic_db.Get(str(uid)))
        except KeyError:
            ori_dict = {}
        
        if len(ori_dict) >= 50:
            # 只更新已有词的词频
            _terms = (set(terms) & set(ori_dict.keys())
            for t in _terms:
                ori_dict[t] += 1
        else:
            # 不断引进新词
            for t in terms:
                try:
                    ori_dict[t] += 1
                except KeyError:
                    ori_dict[t] = 1

        daily_profile_person_topic_db.Put(str(uid), json.dumps(ori_dict))

        count += 1


def personInteract2leveldb():
    '''
    count = 0
    ts = te = time.time()
    max_direct = 0
    max_retweeted = 0
    for k, v in daily_profile_person_interact_db.RangeIter():
        interact_dict = json.loads(v)
        direct_dict = interact_dict['direct']
        retweeted_dict = interact_dict['retweeted']
        direct_len = len(direct_dict.keys())
        retweeted_len = len(retweeted_dict.keys())

        if max_direct < direct_len:
            max_direct = direct_len

        if max_retweeted < retweeted_len:
            max_retweeted = retweeted_len

        if count % 10000 == 0:
            te = time.time()
            print k, v
            print max_retweeted, max_direct
            print count, '%s sec' % (te - ts), ' profile person interact ', now_datestr
            ts = te

        count += 1
    '''

    weibos = xapian_search_weibo.iter_all_docs(fields=['user', 'text', 'retweeted_uid'])

    count = 0
    ts = te = time.time()
    for weibo in weibos:
        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts), ' profile person interact ', now_datestr
            ts = te

        uid = int(weibo['user'])
        text = weibo['text']
        retweeted_uid = weibo['retweeted_uid']

        try:
            interact_dict = json.loads(daily_profile_person_interact_db.Get(str(uid)))
            direct_interact = interact_dict['direct']
            retweeted_interact = interact_dict['retweeted']
        except KeyError:
            direct_interact = {}
            retweeted_interact = {}

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

        daily_profile_person_interact_db.Put(str(uid), json.dumps({'direct': direct_interact, 'retweeted': retweeted_interact}))

        count += 1


if __name__ == '__main__':
    '''
    now_datestr = '20130901'
    daily_profile_airoeik_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_person_%s' % now_datestr),
                                                   block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    count = 0
    max_kcount = 0
    max_key = 0
    ts = te = time.time()
    for k, v in daily_profile_airoeik_bucket.RangeIter():
        a, i, r, o, e, inte, keywords = v.split('_\/')
        keywords = json.loads(keywords)
        if len(keywords.keys()) > max_key:
            max_key = len(keywords.keys())
        
        for _k, _v in keywords.iteritems():
            if int(_v) > max_kcount:
                max_kcount = int(_v)

        if count % 10000 == 0:
            print keywords
            print max_kcount, max_key
            te = time.time()
            print count, '%s sec' % (te - ts), '', now_datestr
            ts = te
        count += 1
    '''

    # get datestr
    now_datestr = sys.argv[1] # '20130901'
    
    # init xapian weibo
    xapian_search_weibo = getXapianWeiboByDate(now_datestr)

    daily_profile_person_topic_db = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_person_topic_%s_update' % now_datestr),
                                                    block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    daily_profile_person_interact_db = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_person_interact_%s_update' % now_datestr),
                                                       block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    
    # personInteract2leveldb()
    personTopic2leveldb()

    # init leveldb
    '''
    try:
        shutil.rmtree(os.path.join(LEVELDBPATH, 'linhao_profile_person_%s' % now_datestr))
    except:
        pass
    
    daily_profile_airoeik_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_person_%s_test2' % now_datestr),
                                                   block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    '''
    '''
    daily_profile_domain_keywords_bucket = {}
    for i in range(9, 21):
        try:
            shutil.rmtree(os.path.join(LEVELDBPATH, 'linhao_profile_domain_keywords_%s_%s' % (now_datestr, i)))
        except:
            pass
        daily_profile_domain_keywords_bucket[i] = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_domain_keywords_%s_%s' % (now_datestr, i)),
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
    '''
    # calculate
    # calc_person_airoeik_xapian2leveldb()
    # calc_domain_airo_keywords()

    #
    # shutil.rmtree(os.path.join(LEVELDBPATH, 'linhao_user2domain_profile_%s' % now_datestr))

    '''
    # sort domain keywords, get topk
    daily_profile_domain_topk_keywords_bucket = {}
    for i in range(9, 21):
        daily_profile_domain_topk_keywords_bucket[i] = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_domain_topk_keywords_%s_%s' % (now_datestr, i)),
                                                                       block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    batch_sort_domain_keywords()
    '''
