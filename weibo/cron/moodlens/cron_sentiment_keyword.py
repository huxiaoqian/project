# -*- coding: utf-8 -*-


import json
from config import db
from time_utils import datetime2ts, ts2HourlyTime
from sqlalchemy.exc import IntegrityError
from xapian_weibo.utils import top_keywords, gen_mset_iter
from model import SentimentKeywords, TopWeibos
from dynamic_xapian_weibo import getXapianWeiboByDate
from config import cron_start, cron_end, emotions_kv


Minute = 60
Fifteenminutes = 15 * Minute
Hour = 3600
SixHour = Hour * 6
Day = Hour * 24

TOP_KEYWORDS_LIMIT = 50
TOP_WEIBOS_LIMIT = 50

start_range_ts = datetime2ts(cron_start)
end_range_ts = datetime2ts(cron_end)

RESP_ITER_KEYS = ['_id', 'user', 'retweeted_uid', 'retweeted_mid', 'text', 'timestamp', 'reposts_count', 'bmiddle_pic', 'geo', 'comments_count', 'sentiment', 'terms']


def save_count_results(dic, during, keywords_limit):
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


def save_weibos_results(dic, during, weibos_limit):
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


def top_weibos(get_results, top=TOP_WEIBOS_LIMIT):
    weibos = []
    for r in get_results():
        weibos.append(r)
    return weibos


def sentiment_keywords(xapian_search_weibo, start_ts=start_range_ts, over_ts=end_range_ts, during=Hour, sort_field='reposts_count', save_fields=RESP_ITER_KEYS, w_limit=TOP_WEIBOS_LIMIT, k_limit=TOP_KEYWORDS_LIMIT):
    start_ts = int(start_ts)
    over_ts = int(over_ts)

    over_ts = ts2HourlyTime(over_ts, during)
    interval = (over_ts - start_ts) / during

    for i in range(interval, 0, -1):
    	emotions_data = {}
        emotions_weibo = {}
        
        begin_ts = over_ts - during * i
        end_ts = begin_ts + during
        print begin_ts, end_ts, ' starts calculate'

        query_dict = {
	        'timestamp': {'$gt': begin_ts, '$lt': end_ts},
	    }

        for k, v in emotions_kv.iteritems():
            query_dict['sentiment'] = v
            print xapian_search_weibo.search(query=query_dict, count_only=True)
            mset = xapian_search_weibo.search(query=query_dict, sort_by=[sort_field], max_offset=w_limit, mset_direct=True)
            top_ws = top_weibos(gen_mset_iter(xapian_search_weibo, mset, fields=save_fields), top=w_limit)
            keywords_with_count = top_keywords(gen_mset_iter(xapian_search_weibo, mset, fields=['terms']), top=k_limit)

            emotions_data[v] = [end_ts, keywords_with_count]
            emotions_weibo[v] = [end_ts, top_ws]

            print k, v, ', emotion keywords length: ', len(keywords_with_count), ', emotion weibos length: ', len(top_ws)

        print date, '%s %s saved emotions keywords and weibos' % (begin_ts, end_ts)
        save_count_results(emotions_data, during, TOP_KEYWORDS_LIMIT)
        save_weibos_results(emotions_weibo, during, TOP_WEIBOS_LIMIT)


def cal_sentiment_kcount_by_date(datestr, duration):
    start_ts = datetime2ts(datestr)
    end_ts = start_ts + Day
    datestr = datestr.replace('-', '')
    xapian_search_weibo = getXapianWeiboByDate(datestr)
    sentiment_keywords(xapian_search_weibo, start_ts=start_ts, over_ts=end_ts, during=duration)


if __name__ == '__main__':
    # test mysql write
    import sys
    date = sys.argv[1] # '2013-09-13'
    cal_sentiment_kcount_by_date(date, Fifteenminutes)

    # test mysql read
    
    '''
    xapian_search_weibo = getXapianWeiboByDate('20130901')
    start_ts = datetime2ts('2013-09-01')
    end_ts = datetime2ts('2013-09-02')
    query_dict = {
        'timestamp':{
            '$gt': start_ts,
            '$lt': end_ts
        }
    }
    count = xapian_search_weibo.search(query=query_dict, count_only=True)
    print count
    '''
