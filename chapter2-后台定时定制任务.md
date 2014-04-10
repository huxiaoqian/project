# 后台定时定制任务编写

**作者: linhao.** 

## 第一节 安装指南

本部分需要配合使用Xapian、ZeroMQ、Redis、Mysql等。

### Redis 安装
参考[redis官网](http://redis.io/)
```
easy_install redis
```

### Xapian与ZeroMQ安装
参考[xapian_weibo](http://github.com/MOON-CLJ/xapian_weibo/)


## 第二节 概述
后台定时定制任务主要包括如下两类：
* 后台话题定时计算任务；
* 增量计算任务；
* 24点计算任务

后台话题定时计算任务又可细分为：
* 话题情绪计算任务；
* 话题敏感博主识别任务；
* 话题传播计算；
* 单条微博传播计算

增量计算任务可细分为：
* 情绪增量计算；
* 用户关键词增量计算

24点计算任务可以细分为：
* 关键博主排序整点计算；
* 画像整点计算

## 第三节 增量计算任务

增量计算任务主要分为如下两个阶段：
1、在redis数据库中实时统计每个15分钟时间单元内3类情绪的数量、关键词、关键微博以及每个用户的关键词，代码可参考[xapian_zmq_realtime_work.py](https://github.com/linhaobuaa/xapian_weibo/blob/master/zmq_workspace/xapian_zmq_realtime_work.py)

其中情绪增量计算代码见realtime_sentiment_cal
```
def realtime_sentiment_cal(item):
    sentiment = item['sentiment']
    # global sentiment
    global_r.incr(GLOBAL_SENTIMENT_COUNT % sentiment)

    terms = [term.encode('utf-8') for term in item['terms']]
    terms = filter(lambda x: x not in single_word_whitelist, terms)
    reposts_count = item['reposts_count']

    if reposts_count > TOP_WEIBOS_REPOSTS_COUNT_LIMIT:
        # top weibos
        global_r.zadd(TOP_WEIBO_REPOSTS_COUNT_RANK % sentiment, reposts_count, item['_id'])
        global_r.set(TOP_WEIBO_KEY % item['_id'], zlib.compress(pickle.dumps(item, pickle.HIGHEST_PROTOCOL), zlib.Z_BEST_COMPRESSION))

        for t in terms:
            # top keywords
            global_r.zincrby(TOP_KEYWORDS_RANK % sentiment, t, 1.0)

    flag_set = set()
    for t in terms:
        if t in global_keywords:
            # keyword sentiment
            global_r.incr(KEYWORD_SENTIMENT_COUNT % (t, sentiment))

            if t not in flag_set:
                # keyword top weibos
                global_r.zadd(KEYWORD_TOP_WEIBO_REPOSTS_COUNT_RANK % (t, sentiment), reposts_count, item['_id'])
                global_r.set(TOP_WEIBO_KEY % item['_id'], zlib.compress(pickle.dumps(item, pickle.HIGHEST_PROTOCOL), zlib.Z_BEST_COMPRESSION))

                for tt in terms:
                    # keyword top keywords
                    global_r.zincrby(KEYWORD_TOP_KEYWORDS_RANK % (t, sentiment), tt, 1.0)
                flag_set.add(t)

    domain = user2domain(item['user'])
    if domain != -1 and domain != 20:
        # domain sentiment
        global_r.incr(DOMAIN_SENTIMENT_COUNT % (domain, sentiment))

        # domain top weibos
        global_r.zadd(DOMAIN_TOP_WEIBO_REPOSTS_COUNT_RANK % (domain, sentiment), reposts_count, item['_id'])
        global_r.set(TOP_WEIBO_KEY % item['_id'], zlib.compress(pickle.dumps(item, pickle.HIGHEST_PROTOCOL), zlib.Z_BEST_COMPRESSION))

        for t in terms:
            # domain top keywords
            global_r.zincrby(DOMAIN_TOP_KEYWORDS_RANK % (domain, sentiment), t, 1.0)
```

其中用户关键词增量计算代码见realtime_profile_keywords_cal
```
def realtime_profile_keywords_cal(item):
    terms_cx = item['terms_cx']
    uid = item['user']
    for term, cx in terms_cx:
        if cx == 'n':
            global_r.zincrby(USER_KEYWORDS % uid, term, 1.0)
            global_r.sadd(USER_SET, uid)
```

2、第二阶段计算任务是将redis中新增的15分钟时间单元数据读出，写入MySQL或Leveldb中，代码可参考cron(https://github.com/linhaobuaa/project/blob/master/weibo/cron/realtime/sentiment_profile_redis.py)

其中情绪计算可参考calc_sentiment方法，用户关键词计算可参考calc_profile方法


