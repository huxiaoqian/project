# -*- coding: utf-8 -*-
'''xapian_zmq_realtime_work中realtime_sentiment_cal需要使用定制话题信息
'''

import redis
from config import REDIS_HOST, REDIS_PORT
from sentiment_topics import _all_topics

SENTIMENT_TOPIC_KEYWORDS = "sentiment_topic_keywords"


def _default_redis(host=REDIS_HOST, port=REDIS_PORT, db=0):
    return redis.StrictRedis(host, port, db)


def _utf8_encode(s):
    if isinstance(s, str):
        return s
    else:
        return s.encode('utf-8')


def _clear_keywords(r):
	r.delete(SENTIMENT_TOPIC_KEYWORDS)


def _add_keyword(r, k):
	r.sadd(SENTIMENT_TOPIC_KEYWORDS, _utf8_encode(k))


if __name__ == '__main__':
    r = _default_redis()
    
    topics = _all_topics(True)
    _clear_keywords(r)
    for topic in topics: 
        _add_keyword(r, topic.topic)