# -*- coding: utf-8 -*-


import json
import datetime
from topics import _all_topics
from time_utils import datetime2ts, ts2HourlyTime, ts2datetime
from xapian_weibo.utils import top_keywords, gen_mset_iter
from dynamic_xapian_weibo import getXapianWeiboByDate, getXapianWeiboByDuration
from config import cron_start, cron_end, emotions_kv, db
from model import SentimentRtTopicCount, SentimentRtTopicKeywords, \
                  SentimentRtTopicTopWeibos
from topicStatus import _topic_not_calc


Minute = 60
Fifteenminutes = 15 * 60
Hour = 3600
SixHour = Hour * 6
Day = Hour * 24

TOP_WEIBOS_LIMIT = 50
TOP_KEYWORDS_LIMIT = 50

SORT_FIELD = 'reposts_count'
RESP_ITER_KEYS = ['_id', 'user', 'retweeted_uid', 'retweeted_mid', 'text', 'timestamp', 'reposts_count', 'bmiddle_pic', 'geo', 'comments_count', 'sentiment', 'terms']


def top_weibos(get_results, top=TOP_WEIBOS_LIMIT):
    weibos = []
    for r in get_results():
        weibos.append(r)
    return weibos


def save_rt_results(calc, query, results, during, klimit=TOP_KEYWORDS_LIMIT, wlimit=TOP_WEIBOS_LIMIT):
    if calc == 'count':
        for k, v in results.iteritems():
            sentiment = k
            ts, count = v
            item = SentimentRtTopicCount(query, during, ts, sentiment, count)
            item_exist = db.session.query(SentimentRtTopicCount).filter(SentimentRtTopicCount.query==query, \
                                                                         SentimentRtTopicCount.range==during, \
                                                                         SentimentRtTopicCount.end==ts, \
                                                                         SentimentRtTopicCount.sentiment==sentiment).first()
            if item_exist:
                db.session.delete(item_exist)
            db.session.add(item)
        
        db.session.commit()

    if calc == 'kcount':
        for k, v in results.iteritems():
            sentiment = k
            ts, kcount = v
            item = SentimentRtTopicKeywords(query, during, klimit, ts, sentiment, json.dumps(kcount))
            item_exist = db.session.query(SentimentRtTopicKeywords).filter(SentimentRtTopicKeywords.query==query, \
                                                                            SentimentRtTopicKeywords.range==during, \
                                                                            SentimentRtTopicKeywords.end==ts, \
                                                                            SentimentRtTopicKeywords.limit==klimit, \
                                                                            SentimentRtTopicKeywords.sentiment==sentiment).first()
            if item_exist:
                db.session.delete(item_exist)
            db.session.add(item)
        
        db.session.commit()

    if calc == 'weibos':
        for k, v in results.iteritems():
            sentiment = k
            ts, weibos = v
            item = SentimentRtTopicTopWeibos(query, during, wlimit, ts, sentiment, json.dumps(weibos))
            item_exist = db.session.query(SentimentRtTopicTopWeibos).filter(SentimentRtTopicTopWeibos.query==query, 
                                                                                   SentimentRtTopicTopWeibos.range==during, 
                                                                                   SentimentRtTopicTopWeibos.end==ts, 
                                                                                   SentimentRtTopicTopWeibos.limit==wlimit, 
                                                                                   SentimentRtTopicTopWeibos.sentiment==sentiment).first()
            if item_exist:
                db.session.delete(item_exist)
            db.session.add(item)
        
        db.session.commit()


def sentimentRealTimeTopic(xapian_search_weibo, query, start_ts, end_ts, save_fields=RESP_ITER_KEYS, during=Fifteenminutes, calc='all', w_limit=TOP_WEIBOS_LIMIT, k_limit=TOP_KEYWORDS_LIMIT, sort_field=SORT_FIELD):
    if query and query != '':

    	start_ts = int(start_ts)
    	over_ts = int(end_ts)
    	
        over_ts = ts2HourlyTime(over_ts, during)
        interval = (over_ts - start_ts) / during

        for i in range(interval, 0, -1):

	    emotions_count = {}
	    emotions_kcount = {}
	    emotions_weibo = {}

	    begin_ts = over_ts - during * i
            end_ts = begin_ts + during
            print begin_ts, end_ts, 'topic not customed %s starts calculate' % query

            query_dict = {
                'timestamp': {'$gt': begin_ts, '$lt': end_ts},
                '$or': []
            }

            for term in query.strip().split(','):
                if term:
                    query_dict['$or'].append({'text': [term]})

            if calc == 'all':
                for k, v in emotions_kv.iteritems():
                    query_dict['sentiment'] = v
                    scount = xapian_search_weibo.search(query=query_dict, count_only=True)
                    mset = xapian_search_weibo.search(query=query_dict, sort_by=[sort_field], \
                                                      max_offset=w_limit, mset_direct=True)
                    kcount = top_keywords(gen_mset_iter(xapian_search_weibo, mset, fields=['terms']), top=k_limit)
                    top_ws = top_weibos(gen_mset_iter(xapian_search_weibo, mset, fields=save_fields), top=w_limit)
                    emotions_count[v] = [end_ts, scount]
                    emotions_kcount[v] = [end_ts, kcount]
                    emotions_weibo[v] = [end_ts, top_ws]

                    print k, v, ', emotions count: ', emotions_count, ', keywords length: ', len(kcount), ', weibos count: ', len(top_ws)
                
                print 'save emotions count, keywords and weibo'
                save_rt_results('count', query, emotions_count, during)
                save_rt_results('kcount', query, emotions_kcount, during, klimit=k_limit)
                save_rt_results('weibos', query, emotions_weibo, during, wlimit=w_limit)
                
            elif calc == 'count':
                for k, v in emotions_kv.iteritems():
                    query_dict['sentiment'] = v
                    scount = xapian_search_weibo.search(query=query_dict, count_only=True)
                    emotions_count[v] = [end_ts, scount]

                save_rt_results('count', query, emotions_count, during)
            
            else:
                if calc == 'kcount':
                    for k, v in emotions_kv.iteritems():
                        query_dict['sentiment'] = v
                        count, get_results = xapian_search_weibo.search(query=query_dict, fields=RESP_ITER_KEYS, \
                                                                            sort_by=[SORT_FIELD], max_offset=w_limit)
                        kcount = top_keywords(get_results, top=k_limit)
                        emotions_kcount[v] = [end_ts, kcount]
                    
                    save_rt_results('kcount', query, emotions_kcount, during, TOP_KEYWORDS_LIMIT)

                if calc == 'weibos':
                    for k, v in emotions_kv.iteritems():
                        query_dict['sentiment'] = v
                        count, get_results = xapian_search_weibo.search(query=query_dict, fields=RESP_ITER_KEYS, \
                                                                        sort_by=[sort_field], max_offset=w_limit)
                        top_ws = top_weibos(get_results, top=w_limit)
                        emotions_weibo[v] = [end_ts, top_ws]
                    
                    save_rt_results('weibos', query, emotions_weibo, during, TOP_WEIBOS_LIMIT)
    

def topic_not_calc():
    topics = _topic_not_calc()
    for topic in topics:
        query = topic.topic
    	end_ts = topic.end
    	during = topic.range
        
        start_ts = end_ts - during
        start_datestr = ts2datetime(start_ts)
        end_datestr = ts2datetime(end_ts)

        ts_1 = datetime2ts(start_datestr)
        ts_2 = datetime2ts(end_datestr)
        days = (ts_2 - ts_1) / Day

        date_list = []
        for i in range(0, days):
            datestr = datetime.date.fromtimestamp(ts_1 + i * Day).isoformat()
            date_list.append(datestr)
            datestr = datestr.replace('-', '')
            xapian_search_weibo = getXapianWeiboByDate(datestr)
            print xapian_search_weibo
            if xapian_search_weibo:
                sentimentRealTimeTopic(xapian_search_weibo, query, ts_1 + i * Day, ts_1 + (i + 1) * Day)
                

if __name__ == '__main__':
  	topic_not_calc()
