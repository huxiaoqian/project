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
from config import db
from model import ProfilePersonBasic, ProfilePersonWeiboCount, ProfilePersonTopic
from config import xapian_search_user, LEVELDBPATH, REDIS_HOST, REDIS_PORT, DOMAIN_LIST, COBAR_HOST, COBAR_PORT, COBAR_USER
from dynamic_xapian_weibo import getXapianWeiboByDate
from xapian_weibo.utils import load_scws, cut

USER_DOMAIN = "user_domain" # user domain hash,
xapian_user_fields = ['_id', 'province', 'city', 'verified', 'name', 'friends_count', \
                     'gender', 'profile_image_url', 'verified_type','followers_count', \
                     'location', 'statuses_count', 'description', 'created_at']
try:
    spieduser_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'spiedusers_3'),
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


def userLeveldb2DomainZh(uid, updatetime='20131220'):
    try:
        v = spieduser_bucket.Get(str(uid) + '_' + str(updatetime))
        domain = v
    except KeyError:
        domain = '其他'

    return domain


def get_daily_user_count_db_by_date(datestr):
    # datestr '20140105'
    daily_user_db = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_person_count_%s_l2m' % datestr),
                                    block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    return daily_user_db


def get_daily_user_basic_db_by_date(datestr):
    # datestr '20140105'
    daily_user_db = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_person_basic_%s' % datestr),
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
    daily_user_db = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_profile_person_%s_test' % datestr),
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

def iter_userbasic2leveldb():
    users = xapian_search_user.iter_all_docs(fields=xapian_user_fields)

    count = 0
    batch = leveldb.WriteBatch()
    ts = te = time.time()
    for user in users:
        if count % 10000 == 0:
            te = time.time()
            daily_profile_person_basic_db.Write(batch, sync=True)
            batch = leveldb.WriteBatch()
            print count, '%s sec' % (te - ts), 'xapian2leveldb person basic'
            ts = te
        # extraction and transfer
        try:
            userId = int(user['_id'])
        except:
            count += 1
            continue
        province = user['province']
        city = user['city']
        verified = user['verified']
        name = _utf_encode(user['name'])
        friendsCount = user['friends_count']
        gender = user['gender']
        profileImageUrl = user['profile_image_url']
        verifiedType = user['verified_type']
        followersCount = user['followers_count']
        location = _utf_encode(user['location'])
        statusesCount = user['statuses_count']
        description = _utf_encode(user['description'])
        domain = userLeveldb2DomainZh(userId)
        
        try:
            created_at = int(user['created_at'])
        except:
            count += 1
            continue

        date = batch_date_1
        #Load
        key = str(userId)
        value = '_\/'.join([str(province), str(city), str(verified), \
                            str(name), str(friendsCount), str(gender), \
                            str(profileImageUrl), str(verifiedType), \
                            str(followersCount), str(location), \
                            str(statusesCount), str(description), \
                            str(created_at), str(domain)])
        batch.Put(key, value)

        count += 1


def iter_userbasic2mysql(cobar_conn, sharding=False):
    cursor = cobar_conn.cursor()

    # drop non-primary indexes on table
    try:
        cursor.execute("SHOW INDEX FROM profile_person_basic")
        results = cursor.fetchall()
        for r in results:
            index_name = r[2]
            if index_name != 'PRIMARY':
                cursor.execute("DROP INDEX %s ON profile_person_basic" % index_name)
    except Exception, e:
        print e
    print 'drop indexes completely'

    # delete data from table
    try:
        cursor.execute("DELETE FROM profile_person_basic")
        cobar_conn.commit()
    except Exception, e:
        cobar_conn.rollback()
        print e
    print 'clean table data completely'

    # insert new data
    count = 0
    ts = te = time.time()
    for k, v in daily_profile_person_basic_db.RangeIter():
        if count % 2000 == 0:
            if sharding:
                # Commit your changes in the database
                cobar_conn.commit()

        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts), 'leveldb2mysql person basic'
            ts = te

        userId = int(k)
        
        try:
    	    province, city, verified, name, friendsCount, gender, profileImageUrl, verifiedType, followersCount, location, statusesCount, description, created_at, domain = v.split('_\/')
        except Exception, e:
            print e
            count += 1
            continue
        description = _utf_encode(description)
        domain = _utf_encode(domain)
        verified = 1 if verified == 'True' else 0
        date = batch_date_1
        
        if sharding:
            sql = """insert into profile_person_basic(userId, province, city, verified, name, gender, \
                     profileImageUrl, verifiedType, friendsCount, followersCount, statuseCount, location, \
                     created_at, description, domain) values ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', 
                     '%s', '%s', '%s', '%s', '%s', '%s', '%s')""" % (userId, province, city, verified, name, gender, \
                     profileImageUrl, verifiedType, friendsCount, followersCount, statusesCount, location, \
                     created_at, description, domain)

            try:
                cursor.execute(sql)
            except Exception, e:
                #print e
                description = ''
                sql = """insert into profile_person_basic(userId, province, city, verified, name, gender, \
                     profileImageUrl, verifiedType, friendsCount, followersCount, statuseCount, location, \
                     created_at, description, domain) values ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', 
                     '%s', '%s', '%s', '%s', '%s', '%s', '%s')""" % (userId, province, city, verified, name, gender, \
                     profileImageUrl, verifiedType, friendsCount, followersCount, statusesCount, location, \
                     created_at, description, domain)
            
                cursor.execute(sql)

        count += 1

    # create index
    print 'create cbw_ppb_userid index on table profile_person_basic'
    try:
        cursor.execute("CREATE INDEX cbw_ppb_userid on profile_person_basic(userId)")
    except Exception, e:
        print e

    print 'create cbw_ppb_fol_fri_sta_loc index on table profile_person_basic'
    try:
        cursor.execute("CREATE INDEX cbw_ppb_fol_fri_sta_loc on profile_person_basic(followersCount, friendsCount, statuseCount, location)")
    except Exception, e:
        print e

    print 'create cbw_ppb_created index on table profile_person_basic'
    try:
        cursor.execute("CREATE INDEX cbw_ppb_created on profile_person_basic(created_at)")
    except Exception, e:
        print e

    print 'create cbw_ppb_fol_domain index on table profile_person_basic'
    try:
        cursor.execute("CREATE INDEX cbw_ppb_fol_domain on profile_person_basic(followersCount, domain)")
    except Exception, e:
        print e

    if cobar_conn:
        cobar_conn.close()


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
            counts = daily_profile_person_count_db.Get(k)
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
            print count, '%s sec' % (te - ts), ' profile person interact ', batch_date_1
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
            print count, '%s sec' % (te - ts), ' profile person calc', batch_date_1
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
    # init xapian weibo
    import sys
    batch_date_1 = sys.argv[1] # '20130905'
    xapian_search_weibo = getXapianWeiboByDate(batch_date_1)

    #
    seed_set = get_official_seed_set()
    scws = load_scws()
    
    # update person basics once a week
    # sharding = False
    # if sharding:
    #    # mysqldb连接数据库　
    #    try:
    #        cobar_conn = MySQLdb.connect(host=COBAR_HOST, user=COBAR_USER, db='cobar_db_weibo', port=COBAR_PORT, charset='utf8')
    #        print 'connection success'
    #    except Exception, e:
    #        print e
    #        sys.exit()
    # daily_profile_person_basic_db = get_daily_user_basic_db_by_date(batch_date_1)
    # iter_userbasic2leveldb()
    # iter_userbasic2mysql(cobar_conn, sharding)
    
    # update person active, important, reposts, original, emoticon once a day
    # daily_profile_person_count_db = get_daily_user_count_db_by_date(batch_date_1)
    # personWeiboCount2levedb()
    
    # update person topic once a day
    # daily_profile_person_topic_db = get_daily_user_topic_db_by_date(batch_date_1)
    # personTopic2leveldb()
    
    # update person interact once a day
    # daily_profile_person_interact_db = get_daily_user_interact_db_by_date(batch_date_1)
    # personInteract2leveldb()
    
    # integration person values into one leveldb
    # daily_profile_person_db = get_daily_user_db_by_date(batch_date_1)
    # batch_handle()
    
    daily_profile_domain_basic_db = get_daily_domain_basic_db_by_date(batch_date_1)
    batch_handle_domain_basic()
    
    '''
    #
    daily_domain_keywords_db = {}
    for i in range(-1, 21):
        daily_domain_keywords_db[i] = get_daily_domain_keywords_db_by_date(batch_date_1, i)
    daily_profile_domain_db = get_daily_domain_db_by_date(batch_date_1)
    batch_handle_domain()
    daily_profile_domain_keywords = get_daily_domain_rtkeywords_db_by_date(batch_date_1)
    batch_sort_domain_keywords()
    '''
