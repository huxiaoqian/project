# -*- coding: utf-8 -*-

import os
import redis
import leveldb
import pickle
import zlib
import json
import time
import datetime
from config import db
from xapian_weibo.utils import get_now_db_no
from model import SentimentCount, SentimentKeywords, TopWeibos, \
                  SentimentDomainCount, SentimentDomainKeywords, \
                  SentimentDomainTopWeibos, SentimentTopicCount, \
                  SentimentTopicKeywords, SentimentTopicTopWeibos
from config import emotions_kv, REDIS_HOST, REDIS_PORT, DOMAIN_LIST, LEVELDBPATH


Minute = 60
Fifteenminutes = 15 * 60
Hour = 3600
SixHour = Hour * 6
Day = Hour * 24
TOP_KEYWORDS_LIMIT = 50
TOP_WEIBOS_LIMIT = 50

GLOBAL_SENTIMENT_COUNT = "global:%s"  # sentiment,
KEYWORD_SENTIMENT_COUNT = "keyword:%s:%s"  # keyword, sentiment,
DOMAIN_SENTIMENT_COUNT = "domain:%s:%s"  # domain, sentiment,

TOP_KEYWORDS_RANK = 'top_keywords:%s'  # sentiment,
KEYWORD_TOP_KEYWORDS_RANK = 'keyword:%s:top_keywords:%s'  # keyword, sentiment,
DOMAIN_TOP_KEYWORDS_RANK = 'domain:%s:top_keywords:%s'  # domain, sentiment,

TOP_WEIBO_REPOSTS_COUNT_RANK = "top_weibo_rank:%s"  # sentiment,
KEYWORD_TOP_WEIBO_REPOSTS_COUNT_RANK = "keyword:%s:top_weibo_rank:%s"  # keyword, sentiment,
DOMAIN_TOP_WEIBO_REPOSTS_COUNT_RANK = "domain:%s:top_weibo_rank:%s"  # domain, sentiment,

SENTIMENT_TOPIC_KEYWORDS = "sentiment_topic_keywords"
TOP_WEIBO_KEY = 'top_weibo:%s'  # id,

NOW_DB_START_TS = "now_db_start_ts"  # start ts
LAST_COMPLETE_START_TS = "last_complete_start_ts" # last complete

USER_KEYWORDS = "user_keywords_%s" # user keywords sorted set, uid,
USER_SET = "user_profile" # user set,

def _default_redis(host=REDIS_HOST, port=REDIS_PORT, db=0):
    return redis.StrictRedis(host, port, db)


def save_global_count(dic, during):
    countrange = during
    for k, v in dic.iteritems():
        sentiment = k
        ts, count = v
        item = SentimentCount(range=countrange, ts=ts, sentiment=sentiment, count=count)
        item_exist = SentimentCount.query.filter_by(range=countrange, ts=ts, sentiment=sentiment).first()
        if item_exist:
            db.session.delete(item_exist)
            db.session.add(item)
        else:
            db.session.add(item)
    db.session.commit()


def save_global_keywords(dic, during, keywords_limit):
    countrange = during
    keywordslimit = keywords_limit
    for k, v in dic.iteritems():
        sentiment = k
        ts, keywords_count = v
        item = SentimentKeywords(range=countrange, limit=keywordslimit, ts=ts, sentiment=sentiment, kcount=json.dumps(keywords_count))
        item_exist = SentimentKeywords.query.filter_by(range=countrange, limit=keywordslimit, ts=ts, sentiment=sentiment).first()
        if item_exist:
            db.session.delete(item_exist)
            db.session.add(item)
        else:
            db.session.add(item)
    db.session.commit()


def save_global_weibos(dic, during, weibos_limit):
    countrange = during
    weiboslimit = weibos_limit
    for k, v in dic.iteritems():
        sentiment = k
        ts, weibos = v
        item = TopWeibos(range=countrange, limit=weiboslimit, ts=ts, sentiment=sentiment, weibos=json.dumps(weibos))
        item_exist = TopWeibos.query.filter_by(range=countrange, limit=weiboslimit, ts=ts, sentiment=sentiment).first()
        if item_exist:
            db.session.delete(item_exist)
            db.session.add(item)
        else:
            db.session.add(item)

    db.session.commit()


def save_domain_count(domain, dic, during):
    for k, v in dic.iteritems():
        sentiment = k
        ts, count = v

        item = SentimentDomainCount(domain, during, ts, sentiment, count)
        item_exist = SentimentDomainCount.query.filter_by(domain=domain, range=during, ts=ts, sentiment=sentiment).first()

        if item_exist:
            db.session.delete(item_exist)
            db.session.add(item)
        else:
            db.session.add(item)

    db.session.commit()


def save_domain_kcount(domain, dic, during, limit):
    for k, v in dic.iteritems():
        sentiment = k
        ts, kcount = v

        item = SentimentDomainKeywords(domain, during, limit, ts, sentiment, json.dumps(kcount))
        item_exist = SentimentDomainKeywords.query.filter_by(domain=domain, range=during, limit=limit, ts=ts, sentiment=sentiment).first()

        if item_exist:
            db.session.delete(item_exist)
            db.session.add(item)
        else:
            db.session.add(item)

    db.session.commit()


def save_domain_weibos(domain, dic, during, limit):
    for k, v in dic.iteritems():
        sentiment = k
        ts, weibos = v

        item = SentimentDomainTopWeibos(domain, during, limit, ts, sentiment, json.dumps(weibos))
        item_exist = SentimentDomainTopWeibos.query.filter_by(domain=domain, range=during, limit=limit, ts=ts, sentiment=sentiment).first()

        if item_exist:
            db.session.delete(item_exist)
            db.session.add(item)
        else:
            db.session.add(item)

    db.session.commit()


def save_topic_results(calc, query, results, during, klimit=TOP_KEYWORDS_LIMIT, wlimit=TOP_WEIBOS_LIMIT):
    if calc == 'count':
        for k, v in results.iteritems():
            sentiment = k
            ts, count = v
            item = SentimentTopicCount(query, during, ts, sentiment, count)
            item_exist = db.session.query(SentimentTopicCount).filter(SentimentTopicCount.query==query, \
                                                                         SentimentTopicCount.range==during, \
                                                                         SentimentTopicCount.end==ts, \
                                                                         SentimentTopicCount.sentiment==sentiment).first()
            if item_exist:
                db.session.delete(item_exist)
            db.session.add(item)
        
        db.session.commit()

    if calc == 'kcount':
        for k, v in results.iteritems():
            sentiment = k
            ts, kcount = v
            item = SentimentTopicKeywords(query, during, klimit, ts, sentiment, json.dumps(kcount))
            item_exist = db.session.query(SentimentTopicKeywords).filter(SentimentTopicKeywords.query==query, \
                                                                            SentimentTopicKeywords.range==during, \
                                                                            SentimentTopicKeywords.end==ts, \
                                                                            SentimentTopicKeywords.limit==klimit, \
                                                                            SentimentTopicKeywords.sentiment==sentiment).first()
            if item_exist:
                db.session.delete(item_exist)
            db.session.add(item)
        
        db.session.commit()

    if calc == 'weibos':
        for k, v in results.iteritems():
            sentiment = k
            ts, weibos = v
            item = SentimentTopicTopWeibos(query, during, wlimit, ts, sentiment, json.dumps(weibos))
            item_exist = db.session.query(SentimentTopicTopWeibos).filter(SentimentTopicTopWeibos.query==query, 
                                                                                   SentimentTopicTopWeibos.range==during, 
                                                                                   SentimentTopicTopWeibos.end==ts, 
                                                                                   SentimentTopicTopWeibos.limit==wlimit, 
                                                                                   SentimentTopicTopWeibos.sentiment==sentiment).first()
            if item_exist:
                db.session.delete(item_exist)
            db.session.add(item)
        
        db.session.commit()


def get_keywords():
    r = _default_redis()
    keywords_set = r.smembers(SENTIMENT_TOPIC_KEYWORDS)
    return list(keywords_set)


def get_before_db_no(db_no):
    before_db_no = db_no - 5
    if before_db_no <= 0:
        before_db_no += 15
    return before_db_no


def clear_current_redis():
    print "delete current redis data"
    r.flushdb()
    '''
    keywords = get_keywords()

    for k, v in emotions_kv.iteritems():
        previous_r.delete(GLOBAL_SENTIMENT_COUNT % v)
        previous_r.delete(TOP_KEYWORDS_RANK % v)
        previous_r.delete(TOP_WEIBO_REPOSTS_COUNT_RANK % v)

    for keyword in keywords:
        for k, v in emotions_kv.iteritems():
            previous_r.delete(KEYWORD_SENTIMENT_COUNT % (keyword, v))
            previous_r.delete(KEYWORD_TOP_KEYWORDS_RANK % (keyword, v))
            previous_r.delete(KEYWORD_TOP_WEIBO_REPOSTS_COUNT_RANK % (keyword, v))

    for fieldid, field in enumerate(DOMAIN_LIST):
        for k, v in emotions_kv.iteritems():
            previous_r.delete(DOMAIN_SENTIMENT_COUNT % (fieldid, v))
            previous_r.delete(DOMAIN_TOP_KEYWORDS_RANK % (fieldid, v))
            previous_r.delete(DOMAIN_TOP_WEIBO_REPOSTS_COUNT_RANK % (fieldid, v))
    '''


def sentiment_count_redis2mysql(end_ts, during=Fifteenminutes):
    keywords = get_keywords()
    
    global_emotions_data ={}
    for k, v in emotions_kv.iteritems():
        global_count = r.get(GLOBAL_SENTIMENT_COUNT % v)
        if not global_count:
            global_count = 0
        global_emotions_data[v] = [end_ts, global_count]
    print '%s global saved count: %s' % (time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())), len(global_emotions_data))
    save_global_count(global_emotions_data, during)

    for keyword in keywords:
        topic_emotions_data = {}
        for k, v in emotions_kv.iteritems():
            topic_count = r.get(KEYWORD_SENTIMENT_COUNT % (keyword, v))
            if not topic_count:
                topic_count = 0
            topic_emotions_data[v] = [end_ts, topic_count]
        print '%s topic %s saved count: %s' % (time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())), keyword, len(topic_emotions_data))
        save_topic_results('count', keyword, topic_emotions_data, during)

    for fieldid, field in enumerate(DOMAIN_LIST):
        domain_emotions_data = {}
        for k, v in emotions_kv.iteritems():
            domain_count = r.get(DOMAIN_SENTIMENT_COUNT % (fieldid, v))
            if not domain_count:
                domain_count = 0
            domain_emotions_data[v] = [end_ts, domain_count]
        print '%s domain %s saved: %s' % (time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())), fieldid, len(domain_emotions_data))
        save_domain_count(fieldid, domain_emotions_data, during)        


def sentiment_kcount_redis2mysql(end_ts, during=Fifteenminutes):
    topics = get_keywords()

    global_emotions_data ={}
    for k, v in emotions_kv.iteritems():
        global_emotions_data[v] = [end_ts, r.zrange(TOP_KEYWORDS_RANK % v, 0, 50, desc=True, withscores=True)]
    print '%s global saved keywords: %s' % (time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())), len(global_emotions_data))
    save_global_keywords(global_emotions_data, during, TOP_KEYWORDS_LIMIT) 
    
    for topic in topics:
        topic_emotions_data = {}
        for k, v in emotions_kv.iteritems():
##            print topic, v
##            print r.zrange(KEYWORD_TOP_KEYWORDS_RANK % (topic, v), 0, 50, desc=True, withscores=True)
            topic_emotions_data[v] = [end_ts, r.zrange(KEYWORD_TOP_KEYWORDS_RANK % (topic, v), 0, 50, desc=True, withscores=True)]
        #print topic_emotions_data
        print '%s topic %s saved keywords: %s' % (time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())), topic, len(topic_emotions_data))
        save_topic_results('kcount', topic, topic_emotions_data, during)

    for fieldid, field in enumerate(DOMAIN_LIST):
        domain_emotions_data = {}
        for k, v in emotions_kv.iteritems():
            domain_emotions_data[v] = [end_ts, r.zrange(DOMAIN_TOP_KEYWORDS_RANK % (fieldid, v), 0, 50, desc=True, withscores=True)]
        print '%s domain %s saved keywords: %s' % (time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())), fieldid, len(domain_emotions_data))
        save_domain_kcount(int(fieldid), domain_emotions_data, during, TOP_KEYWORDS_LIMIT)       


def sentiment_weibo_redis2mysql(end_ts, during=Fifteenminutes):
    topics = get_keywords()

    global_emotions_data ={}
    for k, v in emotions_kv.iteritems():
        weiboids = r.zrange(TOP_WEIBO_REPOSTS_COUNT_RANK % v, 0, 50, desc=True, withscores=False)
        weibos = [pickle.loads(zlib.decompress(r.get(TOP_WEIBO_KEY % mid))) for mid in weiboids]
        global_emotions_data[v] = [end_ts, weibos]
    print '%s global saved weibos: %s' % (time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())), len(global_emotions_data))
    save_global_weibos(global_emotions_data, during, TOP_WEIBOS_LIMIT)    
    
    for topic in topics:
        topic_emotions_data = {}
        for k, v in emotions_kv.iteritems():
            weiboids = r.zrange(KEYWORD_TOP_WEIBO_REPOSTS_COUNT_RANK % (topic, v), 0, 50, desc=True, withscores=False)
            weibos = [pickle.loads(zlib.decompress(r.get(TOP_WEIBO_KEY % mid))) for mid in weiboids]
            topic_emotions_data[v] = [end_ts, weibos]
        #print topic_emotions_data
        print '%s topic %s saved weibos: %s' % (time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())), topic, len(topic_emotions_data))
        save_topic_results('weibos', topic, topic_emotions_data, during)
        
    for fieldid, field in enumerate(DOMAIN_LIST):
        domain_emotions_data = {}
        for k, v in emotions_kv.iteritems():
            weiboids = r.zrange(DOMAIN_TOP_WEIBO_REPOSTS_COUNT_RANK % (fieldid, v), 0, 50, desc=True, withscores=False)
            weibos = [pickle.loads(zlib.decompress(r.get(TOP_WEIBO_KEY % mid))) for mid in weiboids]    
            domain_emotions_data[v] = [end_ts, weibos]
        print '%s domain %s saved weibos: %s' % (time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())), fieldid, len(domain_emotions_data))
        save_domain_weibos(int(fieldid), domain_emotions_data, during, TOP_WEIBOS_LIMIT)


def get_now_datestr(ts):
    realtime = datetime.datetime.fromtimestamp(int(ts)).strftime("%Y%m%d")
    zero_time = datetime.datetime.fromtimestamp(int(ts) - 1).strftime("%Y%m%d")
    if zero_time != realtime:
        return zero_time 
    
    return realtime


def get_now_leveldb_no(ts):
    local_ts = int(ts) - time.timezone
    return int(local_ts) % (24 * 60 * 60) / (15 * 60)
    

def profile_keywords_redis2leveldb(now_leveldb_no, now_datestr, daily_profile_keywords_bucket):
    cursor, members = r.sscan(USER_SET, cursor=0, count=10000)
    count = 0
    ts = te = time.time()
    while cursor != "0":
        for uid in members:
            if count % 10000 == 0:
                te = time.time()
                print count, '%s sec' % (te - ts), ' profile_keywords_redis2leveldb', now_db_no, now_leveldb_no, now_datestr
                ts = te
            count += 1

            # print uid
            keywords_with_count = r.zrange(USER_KEYWORDS % uid, 0, -1, withscores=True)
            daily_profile_keywords_bucket.Put(str(uid), zlib.compress(pickle.dumps(keywords_with_count, pickle.HIGHEST_PROTOCOL), zlib.Z_BEST_COMPRESSION))

        cursor, members = r.sscan(USER_SET, cursor=cursor, count=10000)


def calc_sentiment():
    print "redis db no now", now_db_no
    print "%s start sentiment " % time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    sentiment_count_redis2mysql(end_ts)
    sentiment_kcount_redis2mysql(end_ts)
    sentiment_weibo_redis2mysql(end_ts)
    print "%s end sentiment" % time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))


def calc_profile():
    print "redis db no now", now_db_no
    print "%s start profile" % time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    now_leveldb_no = get_now_leveldb_no(end_ts)
    now_datestr = get_now_datestr(end_ts)
    print "leveldb no now", now_leveldb_no, now_datestr
    daily_profile_keywords_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, './keywords/linhao_profile_keywords_%s_%s' % (now_datestr, now_leveldb_no)),
                                                    block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    profile_keywords_redis2leveldb(now_leveldb_no, now_datestr, daily_profile_keywords_bucket)
    print "%s end profile" % time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))    


def init_redis():
    # 第一次启动之前，清空15个redis db
    for db_no in range(1, 16):
        _default_redis(db=db_no).flushdb()


if __name__ == '__main__':
    # 执行计算的两个先决条件，其一是启动第一次执行计算任务时，需清空15个redis db；
    # 其二是数据可以分块到达，但是需要按照时间顺序，块与块之间需要保持顺序
    # 其三是NOW_DB_START_TS需要清空
    global_r0 = _default_redis()
    now_db_start_ts = global_r0.get(NOW_DB_START_TS)
    
    if now_db_start_ts:
        now_db_start_ts = int(now_db_start_ts)
        print 'now_db_start_ts', now_db_start_ts
        last_complete_start_ts = global_r0.get(LAST_COMPLETE_START_TS)
        
        if last_complete_start_ts:
            last_complete_start_ts = int(last_complete_start_ts)
            print 'last_complete_start_ts', last_complete_start_ts
            
            # 正常应该更新的情况是，last_complete_start_ts是now_db-3甚至更早单元的起始时间，
            # 所以应该去取last_complete_start_ts往后一个时间段的数据，每次挪一个时间单元
            if last_complete_start_ts <= now_db_start_ts - 60 * 15 * 3:
                # 更新last_complete_start_ts
                last_complete_start_ts += 60 * 15
                global_r0.set(LAST_COMPLETE_START_TS, last_complete_start_ts)

                # 开始计算
                end_ts = last_complete_start_ts + 60 * 15
                now_db_no = get_now_db_no(last_complete_start_ts)
                r = _default_redis(db=now_db_no)
                calc_sentiment()
                calc_profile()
                clear_current_redis()                

            # 当last_complete_start_ts 达到 23:15时，把剩余的两段15分钟时间单元更新计算   
            elif last_complete_start_ts < now_db_start_ts and (datetime.datetime.fromtimestamp(last_complete_start_ts).strftime("%H:%M:%S") ==  '23:15:00' or datetime.datetime.fromtimestamp(last_complete_start_ts).strftime("%H:%M:%S") == '23:30:00'):
                # 更新last_complete_start_ts
                last_complete_start_ts += 60 * 15
                global_r0.set(LAST_COMPLETE_START_TS, last_complete_start_ts)

                # 开始计算
                end_ts = last_complete_start_ts + 60 * 15
                now_db_no = get_now_db_no(last_complete_start_ts)
                r = _default_redis(db=now_db_no)
                calc_sentiment()
                calc_profile()
                clear_current_redis()
                
            else:
                print 'wait new data'

        else:
            # last_complete_start_ts不存在，前2个db开始计算，需要check redis中是否有-2 db开始出现数据
            now_db_no = get_now_db_no(now_db_start_ts)
            now_db_no = now_db_no - 2
            if now_db_no <= 0:
                now_db_no =+ 15
            r = _default_redis(db=now_db_no)
            if r.get(GLOBAL_SENTIMENT_COUNT % '1'):
                # 开始第一次计算, 更新last_complete_start_ts
                last_complete_start_ts = now_db_start_ts - 60 * 15 * 2
                global_r0.set(LAST_COMPLETE_START_TS, last_complete_start_ts)

                # 开始计算
                end_ts = last_complete_start_ts + 60 * 15
                now_db_no = get_now_db_no(last_complete_start_ts)
                r = _default_redis(db=now_db_no)
                calc_sentiment()
                calc_profile()
                clear_current_redis()
