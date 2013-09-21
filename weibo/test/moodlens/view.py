# -*- coding: utf-8 -*-

import sys, os, json, time
import unittest
from flask import Flask, render_template, request
from xapian_weibo.xapian_backend import XapianSearch

from utils import top_keywords, getWeiboByMid, st_variation, find_topN

app = Flask(__name__)

emotions_kv = {'happy': 1, 'angry': 2, 'sad': 3}
xapian_search_sentiment = XapianSearch(path='/opt/xapian_weibo/data/20130807', name='master_timeline_sentiment', schema_version=3)
total_days = 90


def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print '%r args: %s %2.2f sec' % (method.__name__, args, te - ts)
        return result
    return timed

@app.route('/moodlens/data/<area>/')
def data(area='global'):
    """
    /keywords_data 接口已备好，只是差领域数据
    """

    query = request.args.get('query', '')
    query = query.strip()
    ts = request.args.get('ts', '')
    ts = long(ts)
    during = request.args.get('during', 24*3600)
    during = int(during)

    begin_ts = ts - during
    end_ts = ts
    print begin_ts, end_ts

    emotions_data = {}

    query_dict = {
        'timestamp': {'$gt': begin_ts, '$lt': end_ts},
        '$or': [],
    }
    if query:
        for term in query.split(','):
            if term:
                query_dict['$or'].append({'text': [term]})
    for k, v in emotions_kv.iteritems():
        query_dict['sentiment'] = v
        count = xapian_search_sentiment.search(query=query_dict, count_only=True)
        emotions_data[k] = [end_ts * 1000, count]

    return json.dumps(emotions_data)


@app.route('/moodlens/keywords_data/<area>/')
def keywords_data(area='global'):
    """
    /keywords_data 接口已备好，只是差领域数据
    """
    query = request.args.get('query', '')
    query = query.strip()
    ts = request.args.get('ts', '')
    ts = long(ts)
    emotion = request.args.get('emotion', 'all')
    during = request.args.get('during', 24*3600)
    during = int(during)

    begin_ts = ts - during
    end_ts = ts

    query_dict = {
        'timestamp': {'$gt': begin_ts, '$lt': end_ts},
    }
    if emotion != 'all':
        query_dict['sentiment'] = emotions_kv[emotion]
    if query:
        query_dict['$or'] = []
        for term in query.split(','):
            if term:
                query_dict['$or'].append({'text': term})

    count, get_results = xapian_search_sentiment.search(query=query_dict, max_offset=100000, sort_by=['-reposts_count'], fields=['terms'])
    keywords_with_count = top_keywords(get_results, top=50)
    keywords_with_count = [list(i) for i in keywords_with_count]

    return json.dumps(keywords_with_count)


@app.route('/moodlens/weibos_data/<emotion>/<area>/')
def weibos_data(emotion, area='global'):
    """
    此接口差领域数据，并且还跟另外的接口差领域数据检索途径不大一样
    """
    query = request.args.get('query', '')
    query = query.strip()
    ts = request.args.get('ts', '')
    ts = long(ts)
    during = request.args.get('during', 24*3600)
    during = int(during)

    begin_ts = ts - during
    end_ts = ts
    query_dict = {
        'timestamp': {'$gt': begin_ts, '$lt': end_ts},
        #'reposts_count': {'$gt': 100},
        'sentiment': emotions_kv[emotion]
    }
    if query:
        query_dict['$or'] = []
        for term in query.split(','):
            if term:
                query_dict['$or'].append({'text': term})

    count, get_results = xapian_search_sentiment.search(query=query_dict, max_offset=10, sort_by=['-reposts_count'], fields=['_id'])
    data = []
    count = 0
    for r in get_results():
        if count == 10:
            break
        count += 1
        weibo_data = getWeiboByMid(r['_id'], emotion)
        if weibo_data:
            data.append(weibo_data)
    return json.dumps(data)


@app.route('/moodlens/emotionpeak/')
def getPeaks():
    happy_lis = request.args.get('happy', '')
    angry_lis = request.args.get('angry', '')
    sad_lis = request.args.get('sad', '')
    ts_lis = request.args.get('ts', '')
    query = request.args.get('query', '')
    query = query.strip()

    happy_lis = [float(da) for da in happy_lis.split(',')]
    angry_lis = [float(da) for da in angry_lis.split(',')]
    sad_lis = [float(da) for da in sad_lis.split(',')]
    ts_lis = [float(da) for da in ts_lis.split(',')]

    sorted_peak_x, peak_x, ts_lis = getPeaksx(happy_lis, angry_lis, sad_lis, ts_lis)
    
    return json.dumps(getPeaksInfo(sorted_peak_x, peak_x, ts_lis, query))

@timeit
def getPeaksx(happy_lis, angry_lis, sad_lis, ts_lis):
    topN = 10
    sentiment_variation = st_variation(happy_lis, angry_lis, sad_lis)
    ##peak_x返回前N个点的在list中的序数0,1.
    ##peak_y返回前N个点的情绪波动值
    peak_x, peak_y = find_topN(sentiment_variation,topN)
    sorted_peak_x = sorted(peak_x)
    return sorted_peak_x, peak_x, ts_lis

@timeit
def getPeaksInfo(sorted_peak_x, peak_x, ts_lis, query):
    time_lis = {}
    for i in peak_x:
        ts = ts_lis[i]
        during = 24 * 3600
        begin_ts = ts - during
        end_ts = ts
        title_text = {'happy': [], 'angry': [], 'sad': []}
        title = {'happy': 'A', 'angry': 'B', 'sad': 'C'}
        for emotion in emotions_kv.keys():
            query_dict = {
                'timestamp': {'$gt': begin_ts, '$lt': end_ts},
                'sentiment': emotions_kv[emotion],
                '$or': []
            }
            for term in query.split(','):
                if term:
                    query_dict['$or'].append({'text': [term]})     
            count, get_results = xapian_search_sentiment.search(query=query_dict, fields=['terms', 'text', 'user'])
            keywords_with_10count = top_keywords(get_results, top=10)
            title_text[emotion] = ','.join([tp[0] for tp in keywords_with_10count])
            title[emotion] = title[emotion] + str(sorted_peak_x.index(i))

        time_lis[i] = {
            'ts': end_ts * 1000,
            'title': title,
            'text': title_text
        }

    return time_lis


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, port=9003)