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
from cron_topic_sentiment import save_rt_results as save_topic_count


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
DOMAIN_TOP_KEYWORDS_RANK = 'domain:%s:top_keywords:%s'  # domain, sentiment,

TOP_WEIBO_REPOSTS_COUNT_RANK = "top_weibo_rank:%s"  # sentiment,
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

    global_emotions_data ={}
    topic_emotions_data = {}
    domain_emotions_data = {}

    keywords = get_keywords()

    for k, v in emotions_kv.iteritems():
        global_count = r.get(GLOBAL_SENTIMENT_COUNT % v)
        if not global_count:
            global_count = 0
        global_emotions_data[v] = [end_ts, global_count]
        for keyword in keywords:
            topic_count = r.get(KEYWORD_SENTIMENT_COUNT % (keyword, v))
            if not topic_count:
                topic_count = 0
            topic_emotions_data[v] = [end_ts, topic_count]
        for field, fieldid in fields_id.iteritems():
            domain_count = r.get(DOMAIN_SENTIMENT_COUNT % (fieldid-1, v))
            if not domain_count:
                domain_count = 0
            domain_emotions_data[v] = [end_ts, domain_count]
            
    print 'global saved: ', global_emotions_data
    save_global_count(global_emotions_data, during)

    for keyword in keywords:
        print 'topic %s saved: ' % keyword, topic_emotions_data
    	save_topic_count('count', keyword, topic_emotions_data, during)

    for field, fieldid in fields_id.iteritems():
        print 'domain %s saved: ' % (fieldid-1), domain_emotions_data
        save_domain_count(int(fieldid), domain_emotions_data, during)


def sentiment_kcount_redis2mysql(r, end_ts, during=Fifteenminutes):
    global_emotions_data ={}
    topic_emotions_data = {}
    domain_emotions_data = {}

    topics = get_keywords()

    for k, v in emotions_kv.iteritems():
        global_emotions_data[v] = [end_ts, r.zrange(TOP_KEYWORDS_RANK % v, 0, 50, desc=True, withscores=True)]
    
        for topic in topics:
            pass
            #topic_emotions_data[v] = [end_ts, r.zrange(TOP_KEYWORDS_RANK % v, 0, 50, desc=True, withscores=True)]
        
        for field, fieldid in fields_id.iteritems():        
            domain_emotions_data[v] = [end_ts, r.zrange(DOMAIN_TOP_KEYWORDS_RANK % (v, fieldid), 0, 50, desc=True, withscores=True)]

    print 'global saved: ', global_emotions_data
    save_global_keywords(global_emotions_data, during, TOP_KEYWORDS_LIMIT)

    for topic in topics:
        print 'topic %s saved: ' % topic, topic_emotions_data
        #save_topic_kcount('count', keyword, topic_emotions_data, during)

    for field, fieldid in fields_id.iteritems():
        print 'domain %s saved: ' % (fieldid-1), domain_emotions_data
        save_domain_kcount(int(fieldid), domain_emotions_data, during, TOP_KEYWORDS_LIMIT)


def sentiment_weibo_redis2mysql(r, end_ts, during=Fifteenminutes):
    global_emotions_data ={}
    topic_emotions_data = {}
    domain_emotions_data = {}

    topics = get_keywords()

    for k, v in emotions_kv.iteritems():
        weiboids = r.zrange(TOP_WEIBO_REPOSTS_COUNT_RANK % v, 0, 50, desc=True, withscores=False)
        weibos = [pickle.loads(zlib.decompress(r.get(TOP_WEIBO_KEY % mid))) for mid in weiboids]
        global_emotions_data[v] = [end_ts, weibos]
    
        for topic in topics:
            pass
            #topic_emotions_data[v] = [end_ts, r.zrange(TOP_KEYWORDS_RANK % v, 0, 50, desc=True, withscores=True)]
        
        for field, fieldid in fields_id.iteritems():
            weiboids = r.zrange(DOMAIN_TOP_WEIBO_REPOSTS_COUNT_RANK % (v, fieldid), 0, 50, desc=True, withscores=False)
            weibos = [pickle.loads(zlib.decompress(r.get(TOP_WEIBO_KEY % mid))) for mid in weiboids]    
            domain_emotions_data[v] = [end_ts, weibos]

    print 'global saved: ', global_emotions_data
    save_global_weibos(global_emotions_data, during, TOP_WEIBOS_LIMIT)

    for topic in topics:
        print 'topic %s saved: ' % topic, topic_emotions_data
        #save_topic_weibos('weibos', topic, topic_emotions_data, during)

    for field, fieldid in fields_id.iteritems():
        print 'domain %s saved: ' % (fieldid-1), domain_emotions_data
        save_domain_weibos(int(fieldid), domain_emotions_data, during, TOP_WEIBOS_LIMIT)
    

if __name__ == '__main__':
    now_db_no = get_now_db_no()
    end_ts = get_now_save_ts()
    print "redis db no now", now_db_no

    r = _default_redis(db=now_db_no)
    sentiment_count_redis2mysql(r, end_ts)
    sentiment_kcount_redis2mysql(r, end_ts)
    sentiment_weibo_redis2mysql(r, end_ts)