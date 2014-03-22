# -*- coding: utf-8 -*-


from dynamic_xapian_weibo import getXapianWeiboByDate
from config import emotions_kv
from time_utils import datetime2ts, ts2HourlyTime
from config import db
from model import SentimentCount
from sqlalchemy.exc import IntegrityError


Minute = 60
Fifteenminutes = 15 * 60
Hour = 3600
SixHour = Hour * 6
Day = Hour * 24


def save_count_results(dic, during):
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


def read_count_results(ts, sentiment, range=Hour):
    item_exist = SentimentCount.query.filter_by(range=range, ts=ts, sentiment=sentiment).first()
    if item_exist:
        return item_exist.count
    else:
        return 0


def sentiment_count(xapian_search_weibo, start_ts, over_ts, during=Hour):
    start_ts = int(start_ts)
    over_ts = int(over_ts)

    over_ts = ts2HourlyTime(over_ts, during)
    interval = (over_ts - start_ts) / during

    for i in range(interval, 0, -1):
        emotions_data = {}

        begin_ts = over_ts - during * i
        end_ts = begin_ts + during
        print begin_ts, end_ts, ' starts calculate'

        query_dict = {
            'timestamp': {'$gt': begin_ts, '$lt': end_ts},
        }

        for k, v in emotions_kv.iteritems():
            query_dict['sentiment'] = v
            count = xapian_search_weibo.search(query=query_dict, count_only=True)
            emotions_data[v] = [end_ts, count]

        print date, 'saved: ', emotions_data 
        save_count_results(emotions_data, during)


def cal_sentiment_count_by_date(datestr, duration):
    start_ts = datetime2ts(datestr)
    end_ts = start_ts + Day
    datestr = datestr.replace('-', '')
    xapian_search_weibo = getXapianWeiboByDate(datestr)
    sentiment_count(xapian_search_weibo, start_ts=start_ts, over_ts=end_ts, during=duration)


if __name__ == '__main__':
    # test mysql write
    import sys
    date = sys.argv[1] # '2013-09-13'
    cal_sentiment_count_by_date(date, Fifteenminutes)
