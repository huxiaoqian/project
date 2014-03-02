# -*- coding: utf-8 -*-

import os
import re
import sys
import time
import json
import redis
import leveldb
import datetime
from xapian_weibo.utils import load_scws, cut
from dynamic_xapian_weibo import getXapianWeiboByDate
from config import LEVELDBPATH, DOMAIN_LIST, xapian_search_user

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
        try:
            direct_interact[repost_user] += 1
        except KeyError:
            direct_interact[repost_user] = 1

    if retweeted_uid != 0:
        try:
            retweeted_interact[retweeted_uid] += 1
        except KeyError:
            retweeted_interact[retweeted_uid] = 1

    interact_dict = {'direct': direct_interact, 'retweeted': retweeted_interact}
    
    terms = cut(scws, _utf_encode(text), f='n')
    for term in terms:
        try:
            keywords_dict[term] += 1
        except KeyError:
            keywords_dict[term] = 1

    return reposts, original, emoticon, interact_dict, keywords_dict


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


def get_airoeik(uid):
    try:
        active, important, reposts, original, emoticon, interact_dict, \
        keywords_dict = daily_profile_airoeik_bucket.Get(str(uid)).split('_\/')
        active = int(active)
        important = int(important)
        reposts = int(reposts)
        original = int(original)
        emoticon = int(emoticon)
        interact_dict = json.loads(interact_dict)
        direct_interact = interact_dict['direct']
        retweeted_interact = interact_dict['retweeted']
        keywords_dict = json.loads(keywords_dict)
    except KeyError:
        active = important = reposts = original = emoticon = 0
        direct_interact = {}
        retweeted_interact = {}
        keywords_dict = {}

    return reposts, original, emoticon, direct_interact, \
           retweeted_interact, keywords_dict


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
    weibos = xapian_search_weibo.iter_all_docs(fields=['user', 'text', 'retweeted_uid', 'retweeted_mid', \
                                                       'reposts_count', 'comments_count'])
    count = 0
    ts = te = time.time()
    for weibo in weibos:
        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts), 'ai xapian to leveldb', now_datestr
            ts = te
        count += 1

        #
        uid = weibo['user']
        reposts_count = weibo['reposts_count']
        retweeted_mid = weibo['retweeted_mid']
        retweeted_uid = weibo['retweeted_uid']
        text = weibo['text']

        # 
        reposts, original, emoticon, direct_interact, retweeted_interact, keywords_dict = get_airoeik(uid)
        reposts, original, emoticon, interact_dict, keywords_dict = calc_roeik(retweeted_mid, retweeted_uid, text, reposts, original, emoticon, direct_interact, retweeted_interact, keywords_dict)

        #
        active, important, follower, domain = get_aifd(uid)
        active, important = calc_ai(active, important, reposts_count)
        
        #
        value = '_'.join([str(active), str(important), str(follower), str(domain)])
        daily_identify_aifd_bucket.Put(str(uid), value)
        value = '_\/'.join([str(active), str(important), str(reposts), str(original), str(emoticon), json.dumps(interact_dict), json.dumps(keywords_dict)])
        daily_profile_airoeik_bucket.Put(str(uid), value)

        # 更新直接转发或原创用户的重要度 + 1，活跃度不变
        retweeted_uid = calc_retweeted_important(retweeted_uid, text)
        if retweeted_uid:
            active, important, follower, domain = get_aifd(retweeted_uid)
            reposts, original, emoticon, direct_interact, retweeted_interact, keywords_dict = get_airoeik(retweeted_uid)
            important += 1

            value = '_'.join([str(active), str(important), str(follower), str(domain)])
            daily_identify_aifd_bucket.Put(str(retweeted_uid), value)

            value = '_\/'.join([str(active), str(important), str(reposts), str(original), str(emoticon), json.dumps(interact_dict), json.dumps(keywords_dict)])
            daily_profile_airoeik_bucket.Put(str(retweeted_uid), value)


def update_follower_xapian2leveldb():
    # 从xapian_search_user更新leveldb的用户粉丝数数据
    users = xapian_search_user.iter_all_docs(fields=['user', 'followers_count'])
    
    count = 0
    ts = te = time.time()
    for user in users:
        uid = user['user']
        try:
            follower = int(user['followers_count'])
        except:
            count += 1
            continue
        
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
            print count, '%s sec' % (te - ts), ' identify person follower', now_datestr
            ts = te
        count += 1


def update_domain2leveldb():
    # 从leveldb更新leveldb的用户领域所属数据
    for k, v in domain_leveldb.RangeIter():
        uid, datestr = k.split('_')
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
            print count, '%s sec' % (te - ts), ' identify person domain', now_datestr
            ts = te
        count += 1


def get_now_datestr():
    return datetime.datetime.now().strftime("%Y%m%d")


if __name__ == '__main__':
    # get datestr
    now_datestr = '20130921'# get_now_datestr()

    # init xapian weibo
    xapian_search_weibo = getXapianWeiboByDate(now_datestr)

    # init leveldb
    daily_identify_aifd_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'yuanshi_daily_count_%s' % now_datestr),
                                                 block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    daily_profile_airoeik_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_person_%s' % now_datestr),
                                                   block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    domain_leveldb = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_user2domain_identify'),
                                                  block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    username_leveldb = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_user_name_identify'),
                                                    block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    
    # calculate
    ai_xapian2leveldb()
    update_follower_xapian2leveldb()
    update_domain2leveldb()
