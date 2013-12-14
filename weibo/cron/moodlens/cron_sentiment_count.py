# -*- coding: utf-8 -*-


from config import cron_start, cron_end, xapian_search_weibo, emotions_kv
from time_utils import datetime2ts
from config import db
from model import SentimentCount
from sqlalchemy.exc import IntegrityError


Minute = 60
Fifteenminutes = 15 * 60
Hour = 3600
SixHour = Hour * 6
Day = Hour * 24

start_range_ts = datetime2ts(cron_start)
end_range_ts = datetime2ts(cron_end)


def ts2HourlyTime(ts, interval):
    # interval 取 Minite、Hour

    ts = ts - ts % interval
    return ts


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


def sentiment_count(start_ts=start_range_ts, over_ts=end_range_ts, during=Hour):
    start_ts = int(start_ts)
    over_ts = int(over_ts)

    over_ts = ts2HourlyTime(over_ts, during)
    interval = (over_ts - start_ts) / during

    for i in range(0, interval):
    	emotions_data = {}

        end_ts = over_ts - during * i
        begin_ts = end_ts - during

        query_dict = {
	        'timestamp': {'$gt': begin_ts, '$lt': end_ts},
	        '$or': [],
	    }

        for k, v in emotions_kv.iteritems():
            query_dict['sentiment'] = v
            count = xapian_search_weibo.search(query=query_dict, count_only=True)
            emotions_data[v] = [end_ts, count]
        
        print 'saved: ', emotions_data
        save_count_results(emotions_data, during)


def test_read_count_results(start_ts=start_range_ts, over_ts=end_range_ts, during=Hour):
    sentiment_results = {}
    over_ts = ts2HourlyTime(over_ts, during)
    interval = (over_ts - start_ts) / during

    for i in range(0, interval):
        end_ts = over_ts - during * i
        
        for k, v in emotions_kv.iteritems():
            count = read_count_results(end_ts, v, Hour)
            sentiment_results[k] = [end_ts*1000, count]
    
    print sentiment_results
    return sentiment_results


if __name__ == '__main__':
    # test mysql write
    start_ts = datetime2ts('2013-09-30')
    end_ts = datetime2ts('2013-10-01')

    sentiment_count(start_ts=start_ts, over_ts=end_ts, during=Fifteenminutes)
    
    # test mysql read
    # start_range_ts = datetime2ts('2013-09-29')
    # end_range_ts = datetime2ts('2013-10-03')
    # test_read_count_results(start_range_ts, end_range_ts)