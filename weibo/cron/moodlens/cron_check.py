# -*- coding: utf-8 -*-


import time
import datetime
from cron_topic_sentiment import sentimentCronTopic
from dynamic_xapian_weibo import getXapianWeiboByDate
from time_utils import datetime2ts, ts2datetime
from topicStatus import _topic_not_calc, _update_topic_status2Computing, _update_topic_status2Completed
from topics import _topic_not_custom_and_expire, _all_topics
from real_time_topic_cron_check import sentimentRealTimeTopic
from sentimentTopic import _drop_sentiment_topic_count, _drop_sentiment_topic_keywords, \
                           _drop_sentiment_topic_weibos


Minute = 60
Fifteenminutes = 15 * 60
Hour = 3600
SixHour = Hour * 6
Day = Hour * 24


def _check_clean_nocustomize_topic():
    '''定时清理定制话题表中iscusotom为False，expiredate小于当前日期的的话题的数量、关键词、关键微博数据
    '''

    topics = _topic_not_custom_and_expire(time.time())
    for topic in topics:
        topicname = topic.topic
        print 'start clean topic nocustomized %s ' % topic
        _drop_sentiment_topic_count(topicname)
        _drop_sentiment_topic_keywords(topicname)
        _drop_sentiment_topic_weibos(topicname)

    print 'end clean topic nocustomized %s ' % topic


def _check_run_customize_topic(cron_start_ts, during=Fifteenminutes):
    '''定时执行定制话题表中iscusotom为True的话题数量、关键词、关键微博计算, 默认当前时间为2013年9月1日，0时0分0秒
    '''

    topics = _all_topics(True)

    for topic in topics:
        topicname = topic.topic
        
        # 本时段计算上个时段的数据
        cron_start_ts = cron_start_ts - cron_start_ts % during
        end_ts = cron_start_ts
        start_ts = end_ts - during
        
        # 本次计算的起始时间作为定位数据库的依据
        print topicname.encode('utf-8'),  ' run customized job from %s to %s ' % (start_ts, end_ts)
        xapian_datestr = datetime.date.fromtimestamp(start_ts).isoformat()
        xapian_search_weibo = getXapianWeiboByDate(xapian_datestr.replace('-', ''))
        
        if xapian_search_weibo:
            sentimentCronTopic(topicname, xapian_search_weibo, start_ts, end_ts)
        else:
            print 'lack xapian weibo data from %s to %s' % (start_ts, end_ts)


def _check_run_notcustomize_topic(during=Fifteenminutes):
    '''定时执行非定制话题表中status为0的话题数量、关键词、关键微博计算
    '''

    topics = _topic_not_calc()
    if topics and len(topics):
        topic = topics[0]
        start_ts = topic.start
        end_ts = topic.end
        topicname = topic.topic
        
        # update status to 0
        _update_topic_status2Computing(topicname, start_ts, end_ts)
        
        print topicname.encode('utf-8'),  ' run realtime job from %s to %s ' % (start_ts, end_ts)
        sentimentRealTimeTopic(topicname, start_ts, end_ts)

        # update status to 1
        _update_topic_status2Completed(topicname, start_ts, end_ts)