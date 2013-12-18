# -*- coding: utf-8 -*-


import json
from config import db
from time_utils import datetime2ts, ts2HourlyTime
from sqlalchemy.exc import IntegrityError
from xapian_weibo.utils import top_keywords
from model import SentimentKeywords, TopWeibos
from dynamic_xapian_weibo import getXapianWeiboByDate
from config import cron_start, cron_end, xapian_search_weibo, emotions_kv


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


def read_count_results(ts, sentiment, range=Hour, limit=TOP_KEYWORDS_LIMIT):
    item_exist = SentimentKeywords.query.filter_by(range=range, ts=ts, limit=limit, sentiment=sentiment).first()
    if item_exist:
        return json.loads(item_exist.kcount)
    else:
        return []


def sentiment_keywords(xapian_weibo=xapian_search_weibo, start_ts=start_range_ts, over_ts=end_range_ts, during=Hour, sort_field='reposts_count', save_fields=RESP_ITER_KEYS, w_limit=TOP_WEIBOS_LIMIT, k_limit=TOP_KEYWORDS_LIMIT):
    start_ts = int(start_ts)
    over_ts = int(over_ts)

    over_ts = ts2HourlyTime(over_ts, during)
    interval = (over_ts - start_ts) / during

    for i in range(0, interval):
    	emotions_data = {}
        emotions_weibo = {}

        end_ts = over_ts - during * i
        begin_ts = end_ts - during

        query_dict = {
	        'timestamp': {'$gt': begin_ts, '$lt': end_ts},
	    }

        for k, v in emotions_kv.iteritems():
            query_dict['sentiment'] = v
            count, get_results = xapian_search_weibo.search(query=query_dict, fields=save_fields, \
                                                            sort_by=[sort_field], max_offset=w_limit)
            keywords_with_count = top_keywords(get_results, top=k_limit)
            top_ws = top_weibos(get_results, top=w_limit)
            emotions_data[v] = [end_ts, keywords_with_count]
            emotions_weibo[v] = [end_ts, top_ws]

        print '%s %s saved emotions keywords and weibos' % (start_ts, over_ts)
        save_count_results(emotions_data, during, TOP_KEYWORDS_LIMIT)
        save_weibos_results(emotions_weibo, during, TOP_WEIBOS_LIMIT)


def test_read_count_results(start_ts=start_range_ts, over_ts=end_range_ts, during=Hour):
    sentiment_results = {}
    over_ts = ts2HourlyTime(over_ts, during)
    interval = (over_ts - start_ts) / during

    for i in range(0, interval):
        end_ts = over_ts - during * i

        for k, v in emotions_kv.iteritems():
            kcount = read_count_results(end_ts, v, during)
            sentiment_results[k] = kcount
            if kcount != []:
                count_list = kcount
                for v1, v2 in count_list:
                    print v1, v2

    return sentiment_results


def sentiment_keywords_search(start_ts, end_ts, during=Hour, limit=TOP_KEYWORDS_LIMIT):
    for k, sentiment in emotions_kv.iteritems():
        items = SentimentKeywords.query.filter(SentimentKeywords.range==during, \
                                                    SentimentKeywords.ts>start_ts, \
                                                    SentimentKeywords.ts<end_ts, \
                                                    SentimentKeywords.limit==limit, \
                                                    SentimentKeywords.sentiment==sentiment).all()
        items = [item.kcount for item in items]
        print items
        for item_exist in items:
            print item_exist
            if item_exist:
                return json.loads(item_exist.kcount)
            else:
                return []



def cal_sentiment_kcount_by_date(datestr, duration):
    start_ts = datetime2ts(datestr)
    end_ts = start_ts + Day
    datestr = datestr.replace('-', '')
    xapian_search_weibo = getXapianWeiboByDate(datestr)
    sentiment_keywords(xapian_weibo=xapian_search_weibo, start_ts=start_ts, over_ts=end_ts, during=duration)


if __name__ == '__main__':
    # test mysql write
    
    '''
    for date in ['2013-09-01', '2013-09-02', '2013-09-03', '2013-09-04', '2013-09-05']:
        cal_sentiment_kcount_by_date(date, Fifteenminutes)
        cal_sentiment_kcount_by_date(date, Day)
    '''

    # test mysql read
    # start_ts = datetime2ts('2013-09-30')
    # end_ts = datetime2ts('2013-10-01')
    # test_read_count_results(start_ts=start_ts, over_ts=end_ts, during=Fifteenminutes)