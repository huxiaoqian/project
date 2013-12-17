# -*- coding: utf-8 -*-


import json
from topics import _all_topics
from time_utils import datetime2ts
from xapian_weibo.utils import top_keywords
from dynamic_xapian_weibo import getXapianWeiboByDate, getXapianWeiboByDuration
from config import cron_start, emotions_kv, xapian_search_weibo, db
from model import SentimentTopicCount, SentimentTopicKeywords, SentimentTopicTopWeibos


TOP_KEYWORDS_LIMIT = 50
TOP_WEIBOS_LIMIT = 50
CRON_START_TS = datetime2ts(cron_start)
RESP_ITER_KEYS = ['_id', 'user', 'retweeted_uid', 'retweeted_mid', 'text', 'timestamp', 'reposts_count', 'bmiddle_pic', 'geo', 'comments_count', 'sentiment', 'terms']
SORT_FIELD = 'reposts_count'


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
            item = SentimentTopicCount(query, during, ts, sentiment, count)
            item_exist = db.session.query(SentimentTopicCount).filter_by(query=query, range=during, end=ts, sentiment=sentiment).first()
            if item_exist:
                db.session.delete(item_exist)
            db.session.add(item)
        
        db.session.commit()

    if calc == 'kcount':
        for k, v in results.iteritems():
            sentiment = k
            ts, kcount = v
            item = SentimentTopicKeywords(query, during, klimit, ts, sentiment, json.dumps(kcount))
            item_exist = db.session.query(SentimentTopicKeywords).filter_by(query=query, range=during, end=ts, limit=klimit, sentiment=sentiment).first()
            if item_exist:
                db.session.delete(item_exist)
            db.session.add(item)
        
        db.session.commit()

    if calc == 'weibos':
        for k, v in results.iteritems():
            sentiment = k
            ts, weibos = v
            item = SentimentTopicTopWeibos(query, during, wlimit, ts, sentiment, json.dumps(weibos))
            item_exist = db.session.query(SentimentTopicTopWeibos).filter_by(query=query, range=during, end=ts, limit=wlimit, sentiment=sentiment).first()
            if item_exist:
                db.session.delete(item_exist)
            db.session.add(item)
        
        db.session.commit()


def sentimentRealTime(xapian_search_weibo, end_ts, during, method='whole', calc='count', query=None, w_limit=TOP_WEIBOS_LIMIT, k_limit=TOP_KEYWORDS_LIMIT):
    emotions_count = {}
    emotions_kcount = {}
    emotions_weibo = {}

    if method == 'topic' and query and query != '':
        query_dict = {
            'timestamp': {'$gt': end_ts-during, '$lt': end_ts},
            '$or': []
        }

        for term in query.strip().split(','):
            if term:
                query_dict['$or'].append({'text': [term]})

        for k, v in emotions_kv.iteritems():
            query_dict['sentiment'] = v

            if calc == 'all':
                scount = xapian_search_weibo.search(query=query_dict, count_only=True)
                count, get_results = xapian_search_weibo.search(query=query_dict, fields=RESP_ITER_KEYS, \
                                                                sort_by=[SORT_FIELD], max_offset=w_limit)
                kcount = top_keywords(get_results, top=k_limit)
                top_ws = top_weibos(get_results, top=w_limit)
                emotions_count[v] = [end_ts, scount]
                emotions_kcount[v] = [end_ts, kcount]
                emotions_weibo[v] = [end_ts, top_ws]
                save_rt_results('count', query, emotions_count, during)
                save_rt_results('kcount', query, emotions_kcount, during, klimit=k_limit)
                save_rt_results('weibos', query, emotions_weibo, during, wlimit=w_limit)
            
            elif calc == 'count':
                scount = xapian_search_weibo.search(query=query_dict, count_only=True)
                emotions_count[v] = [end_ts, scount]
                save_rt_results('count', query, emotions_count, during)
            
            else:
                count, get_results = xapian_search_weibo.search(query=query_dict, fields=RESP_ITER_KEYS, \
                                                                sort_by=[SORT_FIELD], max_offset=w_limit)
                if calc == 'kcount':
                    kcount = top_keywords(get_results, top=k_limit)
                    emotions_kcount[v] = [end_ts, kcount]
                    save_rt_results('kcount', query, emotions_kcount, during, TOP_KEYWORDS_LIMIT)

                if calc == 'weibos':
                    top_ws = top_weibos(get_results, top=w_limit)
                    emotions_weibo[v] = [end_ts, top_ws]
                    save_rt_results('weibos', query, emotions_weibo, during, TOP_WEIBOS_LIMIT)
        
        return {'count': emotions_count, 'kcount': emotions_kcount, 'weibos': emotions_weibo}


def maintain_topic_sentiment(xapian_search_weibo, start_ts, end_ts):
	topics = _all_topics(iscustom=True)
	during = end_ts - start_ts

	for topic in topics:
		topicname = topic.topic
		results = sentimentRealTime(xapian_search_weibo, end_ts, during, method='topic', calc='all', query=topicname)


if __name__ == '__main__':
    start_ts = CRON_START_TS
    end_ts = datetime2ts('2013-09-05')

    datestr_list = ['2013-09-01', '2013-09-02', '2013-09-03', '2013-09-04', '2013-09-05']
    datestr_list = [dl.replace('-', '') for dl in datestr_list]
    xapian_search_weibo = getXapianWeiboByDuration(datestr_list)

    if xapian_search_weibo:
 	maintain_topic_sentiment(xapian_search_weibo, start_ts, end_ts)
