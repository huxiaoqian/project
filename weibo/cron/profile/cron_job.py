# -*- coding: utf-8 -*-

import os
import re
import sys
import time
import json
import redis
import heapq
import leveldb
import MySQLdb
import datetime
from model import ProfilePersonBasic, ProfilePersonWeiboCount, ProfilePersonTopic
from config import db, xapian_search_user, LEVELDBPATH, REDIS_HOST, REDIS_PORT, DOMAIN_LIST, COBAR_HOST, COBAR_PORT, COBAR_USER
from dynamic_xapian_weibo import getXapianWeiboByDate
from xapian_weibo.utils import load_scws, cut

USER_DOMAIN = "user_domain" # user domain hash,
xapian_user_fields = ['_id', 'province', 'city', 'verified', 'name', 'friends_count', \
                     'gender', 'profile_image_url', 'verified_type','followers_count', \
                     'location', 'statuses_count', 'description', 'created_at']
try:
    spieduser_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_user2domain_20140112_4'),
                                       block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
except:
    print 'leveldb not available now'


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


def _default_redis(host=REDIS_HOST, port=REDIS_PORT, db=0):
    return redis.StrictRedis(host, port, db)


def user2domain(uid):
    domainid = global_r0.hget(USER_DOMAIN, str(uid))
    if not domainid:
        domainid = -1 # not taged label
    
    return int(domainid)


def userLeveldb2Domain(uid, updatetime='20131220'):
    try:
        v = spieduser_bucket.Get(str(uid) + '_' + str(updatetime))
        domainid = DOMAIN_LIST.index(v)
    except KeyError:
        domainid = -1

    return domainid


def get_daily_user_count_db_by_date(datestr):
    # datestr '20140105'
    daily_user_db = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_person_count_%s' % datestr),
                                    block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    return daily_user_db


def get_daily_user_topic_db_by_date(datestr):
    # datestr '20140105'
    daily_user_db = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_person_topic_%s' % datestr),
                                    block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    return daily_user_db


def get_daily_user_interact_db_by_date(datestr):
    # datestr '20140105'
    daily_user_db = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_person_interact_%s' % datestr),
                                    block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    return daily_user_db


def get_daily_user_db_by_date(datestr):
    # datestr '20140105'
    daily_user_db = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_person_%s' % datestr),
                                    block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    return daily_user_db


def get_daily_domain_db_by_date(datestr):
    # datestr '20140105'
    daily_user_db = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_domain_%s' % datestr),
                                    block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    return daily_user_db


def get_daily_domain_keywords_db_by_date(datestr, domain):
    # datestr '20140105'
    daily_user_db = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_domain_keywords_%s_%s' % (datestr, domain)),
                                    block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    return daily_user_db


def get_daily_domain_basic_db_by_date(datestr):
    # datestr '20140105'
    daily_user_db = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_domain_basic_%s' % datestr),
                                    block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    return daily_user_db


def get_daily_domain_rtkeywords_db_by_date(datestr):
    # datestr '20140105'
    daily_user_db = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_domain_rtkeywords_%s' % (datestr)),
                                    block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    return daily_user_db


def get_now_datestr():
    return datetime.datetime.now().strftime("%Y-%m-%d")


def _utf_decode(s):
    if isinstance(s, unicode):
        return s
    else:
        return s.decode('utf-8', 'ignore')


def _utf_encode(s):
    if isinstance(s, unicode):
        return s.encode('utf-8', 'ignore')
    else:
        return s


def get_official_seed_set():
    seed_set = set([])
    with open('official_emoticons.txt') as f:
        for l in f:
            seed_set.add(l.rstrip())
    return seed_set


def emoticon_find(text):
    emotion_pattern = r'\[(\S+?)\]'
    remotions = re.findall(emotion_pattern, text)

    emoticons = []
    if remotions:
        for e in remotions:
            if e in seed_set:
                emoticons.append(e.decode('utf-8'))

    return emoticons


def iter_userbasic2mysql(sharding=False):
    cursor = cobar_conn.cursor()
    users = xapian_search_user.iter_all_docs(fields=xapian_user_fields)

    count = 0
    ts = te = time.time()
    for user in users:
        if count % 10000 == 0:
            te = time.time()
            if sharding:
                # Commit your changes in the database
                cobar_conn.commit()
                #except:
                # Rollback in case there is any error
                #cobar_conn.rollback()
            else:
                db.session.commit()
            print count, '%s sec' % (te - ts)
            ts = te

        try:
            userId = int(user['_id'])
        except:
            count += 1
            continue
        province = user['province']
        city = user['city']
        verified = user['verified']
        name = user['name']
        friendsCount = user['friends_count']
        gender = user['gender']
        profileImageUrl = user['profile_image_url']
        verifiedType = user['verified_type']
        followersCount = user['followers_count']
        location = user['location']
        statusesCount = user['statuses_count']
        description = user['description']
        description = _utf_decode(description)
        
        try:
            created_at = int(user['created_at'])
        except:
            count += 1
            continue

        date = now_datetimestr

        if sharding:
            sql = """insert into profile_person_basic(userId, province, city, verified, name, gender, \
                     profileImageUrl, verifiedType, friendsCount, followersCount, statuseCount, location, \
                     description, created_at, date) values ('%s', '%s', '%s', '%d', '%s', '%s', '%s', '%s', 
                     '%s', '%s', '%s', '%s', '%s', '%s', '%s')""" % (userId, province, city, verified, name, gender, \
                     profileImageUrl, verifiedType, friendsCount, followersCount, statusesCount, location, \
                     description, created_at, date)
            try:
                cursor.execute(sql)
            except Exception, e:
                description = ''
                sql = """insert into profile_person_basic(userId, province, city, verified, name, gender, \
                     profileImageUrl, verifiedType, friendsCount, followersCount, statuseCount, location, \
                     description, created_at, date) values ('%s', '%s', '%s', '%d', '%s', '%s', '%s', '%s', 
                     '%s', '%s', '%s', '%s', '%s', '%s', '%s')""" % (userId, province, city, verified, name, gender, \
                     profileImageUrl, verifiedType, friendsCount, followersCount, statusesCount, location, \
                     description, created_at, date)

                cursor.execute(sql)
                
        else:
            '''
            item_exist = db.session.query(ProfilePersonBasic).filter(ProfilePersonBasic.userId==userId).first()
            if item_exist:
                db.session.delete(item_exist)
            '''
            item = ProfilePersonBasic(userId, province, city, verified, name, gender, profileImageUrl, \
                                      verifiedType, friendsCount, followersCount, statusesCount, location, \
                                      description, created_at, date)
            db.session.add(item)

        count += 1


def personWeiboCount2levedb():
    # test 0.6 seconds per 10000 weibos
    weibos = xapian_search_weibo.iter_all_docs(fields=['user', 'retweeted_mid', 'reposts_count', 'comments_count', 'text'])

    count = 0
    ts = te = time.time()
    batch = leveldb.WriteBatch()
    for weibo in weibos:
        if count % 10000 == 0:
            te = time.time()
            daily_profile_person_count_db.Write(batch, sync=True)
            batch = leveldb.WriteBatch()
            print count, '%s sec' % (te - ts)
            ts = te

        uid = weibo['user']
        retweeted_mid = weibo['retweeted_mid']
        reposts_count = weibo['reposts_count']
        comments_count = weibo['comments_count']
        text = weibo['text']

        try:
            counts = daily_profile_person_count_db.Get(str(uid))
            active, important, reposts, original, emoticon = counts.split('_')
            active = int(active)
            important = int(important)
            reposts = int(reposts)
            original = int(original)
            emoticon = int(emoticon)
        except KeyError:
            active = important = reposts = original = emoticon = 0

        active += 1
        important += reposts_count + comments_count

        if retweeted_mid != 0:
            reposts += 1
        else:
            original += 1

        _emoticons = emoticon_find(text)
        if _emoticons and len(_emoticons):
            emoticon += 1

        batch.Put(str(uid), str(active) + '_' + str(important) + '_' + str(reposts) + '_' + str(original) + '_' + str(emoticon))

        count += 1


def personTopic2leveldb(keyword_limit=50):
    # test 0.6 seconds per 10000 weibos
    weibos = xapian_search_weibo.iter_all_docs(fields=['user', 'text'])

    count = 0
    ts = te = time.time()
    batch = leveldb.WriteBatch()
    for weibo in weibos:
        if count % 10000 == 0:
            te = time.time()
            daily_profile_person_topic_db.Write(batch, sync=True)
            batch = leveldb.WriteBatch()
            print count, '%s sec' % (te - ts)
            ts = te

        uid = weibo['user']
        text = _utf_encode(weibo['text'])
        terms = cut(scws, text, f='n')

        try:
            ori_dict = json.loads(daily_profile_person_topic_db.Get(str(uid)))
        except KeyError:
            ori_dict = {}

        for term in terms:
            try:
                ori_dict[term] += 1
            except KeyError:
                ori_dict[term] = 1

        batch.Put(str(uid), json.dumps(ori_dict))

        count += 1


def personInteract2leveldb():
    weibos = xapian_search_weibo.iter_all_docs(fields=['user', 'text', 'retweeted_uid'])

    count = 0
    ts = te = time.time()
    batch = leveldb.WriteBatch()
    for weibo in weibos:
        if count % 10000 == 0:
            te = time.time()
            daily_profile_person_interact_db.Write(batch, sync=True)
            batch = leveldb.WriteBatch()
            print count, '%s sec' % (te - ts)
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

        repost_user = re.search('//@([a-zA-Z-_\u0391-\uFFE5]+)', text)
        if repost_user:
            repost_user = repost_user.group(0)
            try:
                direct_interact[repost_user] += 1
            except KeyError:
                direct_interact[repost_user] = 1

        if retweeted_uid != 0:
            try:
                retweeted_interact[retweeted_uid] += 1
            except KeyError:
                retweeted_interact[retweeted_uid] = 1

        batch.Put(str(uid), json.dumps({'direct': direct_interact, 'retweeted': retweeted_interact}))

        count += 1


def batch_handle():
    weibos = xapian_search_weibo.iter_all_docs(fields=['user', 'text', 'retweeted_uid', \
    'retweeted_mid', 'reposts_count', 'comments_count', 'text'])

    count = 0
    ts = te = time.time()
    batch = leveldb.WriteBatch()
    for weibo in weibos:
        if count % 10000 == 0:
            te = time.time()
            daily_profile_person_db.Write(batch, sync=True)
            batch = leveldb.WriteBatch()
            print count, '%s sec' % (te - ts)
            ts = te

        uid = weibo['user']
        retweeted_mid = weibo['retweeted_mid']
        reposts_count = weibo['reposts_count']
        comments_count = weibo['comments_count']
        text = weibo['text']
        retweeted_uid = weibo['retweeted_uid']

        try:
            results = daily_profile_person_db.Get(str(uid))
            active, important, reposts, original, emoticon, interact_dict, keywords_dict = results.split('_\/')
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

        active += 1
        important += reposts_count + comments_count

        if retweeted_mid != 0:
            reposts += 1
        else:
            original += 1

        _emoticons = emoticon_find(text)
        if _emoticons and len(_emoticons):
            emoticon += 1

        repost_user = re.search('//@([a-zA-Z-_\u0391-\uFFE5]+)', text)
        if repost_user:
            repost_user = repost_user.group(0)
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

        key = str(uid)
        value = '_\/'.join([str(active), str(important), str(reposts), str(original), str(emoticon), json.dumps(interact_dict), json.dumps(keywords_dict)])
        batch.Put(key, value)
        count += 1


def batch_handle_domain_basic():
    count = 0
    ts = te = time.time()
    users = xapian_search_user.iter_all_docs(fields=['_id', 'verified', 'location']) 
    for user in users:
        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts), ' %s daily domain basic' % batch_date_1
            ts = te

        domainid = userLeveldb2Domain(user['_id'])
        verified = user['verified']
        province_str = user['location'].split(' ')[0]
        
        try:
            verified_count, unverified_count, province_dict = daily_profile_domain_basic_db.Get(str(domainid)).split('_\/')
            verified_count = int(verified_count)
            unverified_count = int(unverified_count)
            province_dict = json.loads(province_dict)
        except KeyError:
            verified_count = unverified_count = 0
            province_dict = {}

        if verified:
            verified_count += 1
        else:
            unverified_count += 1

        try:
            province_dict[province_str] += 1
        except KeyError:
            province_dict[province_str] = 1

        key = str(domainid)
        value = '_\/'.join([str(verified_count), str(unverified_count), json.dumps(province_dict)])
        daily_profile_domain_basic_db.Put(key, value)

        count += 1


def batch_sort_domain_keywords(topk=50):    
    for domainid in range(-1, 21):
        print '-----', domainid

        keywords_th = TopkHeap(topk)
        db = daily_domain_keywords_db[domainid]

        for k, v in db.RangeIter():
            v = int(v)
            keywords_th.Push((v, k))

        top_keywords = keywords_th.TopK()
        top_keywords_dict = {}

        for count, keywords in top_keywords:
            top_keywords_dict[keywords] = count

        daily_profile_domain_keywords.Put(str(domainid), json.dumps(top_keywords_dict))


def batch_handle_domain():
    weibos = xapian_search_weibo.iter_all_docs(fields=['user', 'text', \
    'retweeted_mid', 'reposts_count', 'comments_count', 'text'])

    count = 0
    ts = te = time.time()
    batch = leveldb.WriteBatch()
    for weibo in weibos:
        if count % 10000 == 0:
            te = time.time()
            daily_profile_domain_db.Write(batch, sync=True)
            batch = leveldb.WriteBatch()
            print count, '%s sec' % (te - ts), ' %s daily domain' % batch_date_1
            ts = te

        uid = weibo['user']
        domain = userLeveldb2Domain(uid)
        retweeted_mid = weibo['retweeted_mid']
        reposts_count = weibo['reposts_count']
        comments_count = weibo['comments_count']
        text = weibo['text']
        retweeted_mid = weibo['retweeted_mid']

        try:
            results = daily_profile_domain_db.Get(str(domain))
            active, important, reposts, original = results.split('_\/')
            active = int(active)
            important = int(important)
            reposts = int(reposts)
            original = int(original)
        except KeyError:
            active = important = reposts = original = 0

        active += 1
        important += reposts_count + comments_count

        if retweeted_mid != 0:
            reposts += 1
        else:
            original += 1

        key = str(domain)
        value = '_\/'.join([str(active), str(important), str(reposts), str(original)])
        batch.Put(key, value)

        daily_profile_domain_keywords_db = daily_domain_keywords_db[int(domain)]
        terms = cut(scws, _utf_encode(text), f='n')
        for term in terms:
            try:
                kcount = int(daily_profile_domain_keywords_db.Get(str(term)))
                daily_profile_domain_keywords_db.Put(str(term), str(kcount + 1))
            except KeyError:
                daily_profile_domain_keywords_db.Put(str(term), str(1))

        count += 1


if __name__ == '__main__':
    sharding = True

    if sharding:
        # 连接数据库　
        try:
            cobar_conn = MySQLdb.connect(host=COBAR_HOST, user=COBAR_USER, db='cobar_db_weibo', port=COBAR_PORT, charset='utf8')
            print 'connection success'
        except Exception, e:
            print e
            sys.exit()

    now_datetimestr = get_now_datestr()
    iter_userbasic2mysql(sharding)

    if cobar_conn:
        cobar_conn.close()

    '''
    seed_set = get_official_seed_set()
    scws = load_scws()

    batch_date_1 = '20130901'
    xapian_search_weibo = getXapianWeiboByDate(batch_date_1)

    daily_domain_keywords_db = {}
    for i in range(-1, 21):
        daily_domain_keywords_db[i] = get_daily_domain_keywords_db_by_date(batch_date_1, i)

    # daily_profile_person_count_db = get_daily_user_count_db_by_date(batch_date_1)
    # personWeiboCount2levedb()
    
    # daily_profile_person_topic_db = get_daily_user_topic_db_by_date(batch_date_1)
    # personTopic2leveldb()

    # daily_profile_person_interact_db = get_daily_user_interact_db_by_date(batch_date_1)
    # personInteract2leveldb()

    # daily_profile_person_db = get_daily_user_db_by_date(batch_date_1)
    # batch_handle()

    # daily_profile_domain_basic_db = get_daily_domain_basic_db_by_date(batch_date_1)
    # batch_handle_domain_basic()

    # daily_profile_domain_db = get_daily_domain_db_by_date(batch_date_1)
    # batch_handle_domain()

    daily_profile_domain_keywords = get_daily_domain_rtkeywords_db_by_date(batch_date_1)
    batch_sort_domain_keywords()
    '''
