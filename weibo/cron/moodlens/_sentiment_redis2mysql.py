# -*- coding: utf-8 -*-

import redis
import pickle
import zlib
import time
from config import emotions_kv, fields_id, REDIS_HOST, REDIS_PORT
from xapian_weibo.utils import get_now_db_no
from cron_sentiment_count import save_count_results as save_global_count
from cron_sentiment_keyword import save_count_results as save_global_keywords, \
                                   save_weibos_results as save_global_weibos
from cron_field_sentiment import save_count_results as save_domain_count, \
                                 save_kcount_results as save_domain_kcount, \
                                 save_weibos_results as save_domain_weibos
from cron_topic_sentiment import save_rt_results as save_topic_results


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


def _default_redis(host=REDIS_HOST, port=REDIS_PORT, db=0):
    return redis.StrictRedis(host, port, db)


def get_keywords():
    r = _default_redis()
    keywords_set = r.smembers(SENTIMENT_TOPIC_KEYWORDS)
    return list(keywords_set)


def get_now_save_ts(during=Fifteenminutes):
    now_ts =  time.time()
    end_ts = now_ts - now_ts % during + during
    return end_ts


def sentiment_count_redis2mysql(r, end_ts, during=Fifteenminutes):
    keywords = get_keywords()
    
    global_emotions_data ={}
    for k, v in emotions_kv.iteritems():
        global_count = r.get(GLOBAL_SENTIMENT_COUNT % v)
        if not global_count:
            global_count = 0
        global_emotions_data[v] = [end_ts, global_count]
    print 'global saved count: ', global_emotions_data
    save_global_count(global_emotions_data, during)

    for keyword in keywords:
        topic_emotions_data = {}
        for k, v in emotions_kv.iteritems():
            topic_count = r.get(KEYWORD_SENTIMENT_COUNT % (keyword, v))
            if not topic_count:
                topic_count = 0
            topic_emotions_data[v] = [end_ts, topic_count]
        print 'topic %s saved count: ' % keyword, topic_emotions_data
        save_topic_results('count', keyword, topic_emotions_data, during)

    for field, fieldid in fields_id.iteritems():
        domain_emotions_data = {}
        for k, v in emotions_kv.iteritems():
            domain_count = r.get(DOMAIN_SENTIMENT_COUNT % (fieldid-1, v))
            if not domain_count:
                domain_count = 0
            domain_emotions_data[v] = [end_ts, domain_count]
        print 'domain %s saved: ' % (fieldid-1), domain_emotions_data
        save_domain_count(int(fieldid)-1, domain_emotions_data, during)        


def sentiment_kcount_redis2mysql(r, end_ts, during=Fifteenminutes):
    topics = get_keywords()

    global_emotions_data ={}
    for k, v in emotions_kv.iteritems():
        global_emotions_data[v] = [end_ts, r.zrange(TOP_KEYWORDS_RANK % v, 0, 50, desc=True, withscores=True)]
    print 'global saved keywords: '
    save_global_keywords(global_emotions_data, during, TOP_KEYWORDS_LIMIT) 
    
    for topic in topics:
        topic_emotions_data = {}
        for k, v in emotions_kv.iteritems():
            print topic, v
            print r.zrange(KEYWORD_TOP_KEYWORDS_RANK % (topic, v), 0, 50, desc=True, withscores=True)
            topic_emotions_data[v] = [end_ts, r.zrange(KEYWORD_TOP_KEYWORDS_RANK % (topic, v), 0, 50, desc=True, withscores=True)]
        print topic_emotions_data
        print 'topic %s saved keywords: ' % topic
        save_topic_results('kcount', topic, topic_emotions_data, during)

    for field, fieldid in fields_id.iteritems():
        domain_emotions_data = {}
        for k, v in emotions_kv.iteritems():
            domain_emotions_data[v] = [end_ts, r.zrange(DOMAIN_TOP_KEYWORDS_RANK % (v, fieldid), 0, 50, desc=True, withscores=True)]
        print 'domain %s saved keywords: ' % (fieldid-1)
        save_domain_kcount(int(fieldid-1), domain_emotions_data, during, TOP_KEYWORDS_LIMIT)       


def sentiment_weibo_redis2mysql(r, end_ts, during=Fifteenminutes):
    topics = get_keywords()

    global_emotions_data ={}
    for k, v in emotions_kv.iteritems():
        weiboids = r.zrange(TOP_WEIBO_REPOSTS_COUNT_RANK % v, 0, 50, desc=True, withscores=False)
        weibos = [pickle.loads(zlib.decompress(r.get(TOP_WEIBO_KEY % mid))) for mid in weiboids]
        global_emotions_data[v] = [end_ts, weibos]
    print 'global saved weibos: '
    save_global_weibos(global_emotions_data, during, TOP_WEIBOS_LIMIT)    
    
    for topic in topics:
        topic_emotions_data = {}
        for k, v in emotions_kv.iteritems():
            weiboids = r.zrange(KEYWORD_TOP_WEIBO_REPOSTS_COUNT_RANK % (topic, v), 0, 50, desc=True, withscores=False)
            weibos = [pickle.loads(zlib.decompress(r.get(TOP_WEIBO_KEY % mid))) for mid in weiboids]
            topic_emotions_data[v] = [end_ts, weibos]
        print topic_emotions_data
        print 'topic %s saved weibos: ' % topic
        save_topic_results('weibos', topic, topic_emotions_data, during)
        
    for field, fieldid in fields_id.iteritems():
        domain_emotions_data = {}
        for k, v in emotions_kv.iteritems():
            weiboids = r.zrange(DOMAIN_TOP_WEIBO_REPOSTS_COUNT_RANK % (v, fieldid), 0, 50, desc=True, withscores=False)
            weibos = [pickle.loads(zlib.decompress(r.get(TOP_WEIBO_KEY % mid))) for mid in weiboids]    
            domain_emotions_data[v] = [end_ts, weibos]
        print 'domain %s saved weibos: ' % (fieldid-1)
        save_domain_weibos(int(fieldid-1), domain_emotions_data, during, TOP_WEIBOS_LIMIT)
    

if __name__ == '__main__':
    now_db_no = get_now_db_no()
    end_ts = get_now_save_ts()
    print "redis db no now", now_db_no

    r = _default_redis(db=now_db_no)
    sentiment_count_redis2mysql(r, end_ts)
    sentiment_kcount_redis2mysql(r, end_ts)
    sentiment_weibo_redis2mysql(r, end_ts)
