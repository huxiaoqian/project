# -*- coding: utf-8 -*-

from flask import Blueprint, render_template, request
from xapian_weibo.xapian_backend_extra import Schema
from xapian_weibo.xapian_backend import XapianSearch
from utils import top_keywords
import simplejson as json
import datetime
import time
import leveldb
import os
import numpy as np

mod = Blueprint('moodlens', __name__, url_prefix='/moodlens')

LEVELDBPATH = '/home/mirage/leveldb'
buckets = {}

def get_bucket(bucket):
    if bucket in buckets:
        return buckets[bucket]
    buckets[bucket] = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'lijun_' + bucket),
                                      block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    return buckets[bucket]

emotions_kv = {'happy': 1, 'angry': 2, 'sad': 3}
xapian_search_sentiment = XapianSearch(path='/opt/xapian_weibo/data/20130807', name='master_timeline_sentiment', schema_version=3)
xapian_search_weibo = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_weibo')
total_days = 90

@mod.route('/')
def index():
    return render_template('moodlens/index.html', active='moodlens')

@mod.route('/field')
def field():
    return render_template('moodlens/field_emotion.html', active='moodlens')

@mod.route('/topic')
def topic():
    return render_template('moodlens/topic_emotion.html', active='moodlens')

@mod.route('/data/<area>/')
def data(area='global'):
    """
    /keywords_data 接口已备好，只是差领域数据
    """

    query = request.args.get('query', '')
    query = query.strip()
    ts = request.args.get('ts', '')
    ts = long(ts)

    during = 24 * 3600

    emotions_data = {}
    if query:
        begin_ts = ts - during
        end_ts = ts
        print begin_ts, end_ts
        query_dict = {
            'timestamp': {'$gt': begin_ts, '$lt': end_ts},
            '$or': [],
        }
        for term in query.split(','):
            if term:
                query_dict['$or'].append({'text': [term]})
        for k, v in emotions_kv.iteritems():
            query_dict['sentiment'] = v
            count = xapian_search_sentiment.search(query=query_dict, count_only=True)

            emotions_data[k] = [end_ts * 1000, count]
    else:
        bucket = get_bucket('weibo_daily_sentiment_count_%s' % area)
        end_ts = ts

        for k, v in emotions_kv.iteritems():
            try:
                daily_emotion_count = bucket.Get(str(end_ts) + '_' + str(v))
                daily_emotion_count = int(daily_emotion_count)
            except KeyError:
                daily_emotion_count = 0
            emotions_data[k] = [end_ts * 1000, daily_emotion_count]

    return json.dumps(emotions_data)

@mod.route('/flag_data/<emotion>/<area>/')
def flag_data(emotion, area='global'):
    """
    此接口先只供调试用
    /flag_data现在无法连上真实数据，得等拐点识别
    """
    ts = request.args.get('ts', '')
    ts = long(ts)

    today = datetime.datetime.today()
    now_ts = time.mktime(datetime.datetime(today.year, today.month, today.day, 2, 0).timetuple())
    now_ts = long(now_ts)
    during = 24 * 3600

    timestamps = []
    for i in xrange(-total_days + 5, 1, 10):
        timestamps.append(now_ts + during * i)

    during = 3600

    data = []
    if ts in timestamps:
        begin_ts = ts - during
        end_ts = ts
        query_dict = {
            'timestamp': {'$gt': begin_ts, '$lt': end_ts},
            'sentiment': emotions_kv[emotion],
        }
        count, get_results = xapian_search_sentiment.search(query=query_dict, fields=['terms'])
        print count
        keywords_with_count = top_keywords(get_results, top=10)
        text = ','.join([tp[0] for tp in keywords_with_count])
        data.append({
            'x': end_ts * 1000,
            'title': chr(ord('A') + timestamps.index(end_ts)),
            'text': text
        })

    return json.dumps(data)

@mod.route('/keywords_data/<emotion>/<area>/')
def keywords_data(emotion, area='global'):
    """
    /keywords_data 接口已备好，只是差领域数据
    """
    query = request.args.get('query', '')
    query = query.strip()
    ts = request.args.get('ts', '')
    ts = long(ts)

    during = 3600

    begin_ts = ts - during
    end_ts = ts
    query_dict = {
        'timestamp': {'$gt': begin_ts, '$lt': end_ts},
        'sentiment': emotions_kv[emotion],
    }
    if query:
        query_dict['$or'] = []
        for term in query.split(','):
            if term:
                query_dict['$or'].append({'text': term})

    count, get_results = xapian_search_sentiment.search(query=query_dict, fields=['terms'])
    print count
    keywords_with_count = top_keywords(get_results, top=20)
    keywords_with_count = [list(i) for i in keywords_with_count]

    return json.dumps(keywords_with_count)

@mod.route('/weibos_data/<area>/')
def weibos_data(area='global'):
    """
    此接口差领域数据，并且还跟另外的接口差领域数据检索途径不大一样
    """
    query = request.args.get('query', '')
    query = query.strip()
    ts = request.args.get('ts', '')
    ts = long(ts)

    during = 3600

    begin_ts = ts - during
    end_ts = ts
    query_dict = {
        'timestamp': {'$gt': begin_ts, '$lt': end_ts},
        'reposts_count': {'$gt': 1000},
    }
    if query:
        query_dict['$or'] = []
        for term in query.split(','):
            if term:
                query_dict['$or'].append({'text': term})

    count, get_results = xapian_search_weibo.search(query=query_dict, max_offset=5, sort_by=['-reposts_count'], fields=['text', 'timestamp'])
    print count
    data = list(get_results())

    return json.dumps(data)

@mod.route('/emotionpeak/')
def getPeaks():
    happy_lis = request.args.get('happy', '')
    angry_lis = request.args.get('happy', '')
    sad_lis = request.args.get('sad', '')
    ts_lis = request.args.get('ts', '')
    query = request.args.get('query', '')
    query = query.strip()

    happy_lis = [float(da) for da in happy_lis.split(',')]
    angry_lis = [float(da) for da in angry_lis.split(',')]
    sad_lis = [float(da) for da in sad_lis.split(',')]
    ts_lis = [float(da) for da in ts_lis.split(',')]
    topN = 10
    sentiment_variation = st_variation(happy_lis, angry_lis, sad_lis)
    ##peak_x返回前N个点的在list中的序数0,1.
    ##peak_y返回前N个点的情绪波动值
    peak_x,peak_y = find_topN(sentiment_variation,topN)
    time_lis = {}
    for i in peak_x:
        during = 24 * 3600
        ts = ts_lis[i]
        begin_ts = ts - during
        end_ts = ts
        ann_data = {}
        cloud_text = {'happy': [], 'angry': [], 'sad': []}
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
            keywords_with_50count = top_keywords(get_results, top=50)
            keywords_with_10count = top_keywords(get_results, top=10)
            ann_text = ','.join([tp[0] for tp in keywords_with_10count])
            for kw, c in keywords_with_50count:
                cloud_text[emotion].append({'w': kw, 'c': c})
            '''
            ann_text_list = []
            limit_count = 0
            for r in get_results():
                if limit_count > 3:
                    break
                print r['user'], r['text']
                ann_text_list.append('用户<a target="_blank" href="http://weibo.com/u/' + r['user'] + '/>' + r['user'] +'</a>"说：' + r['text'])
                limit_count += 1
            '''
            ann_data[emotion] = {
                'title': emotion,
                'text':  ann_text
            }
        time_lis[i] = {'ts': ts_lis[i], 'ann_data': ann_data, 'cloud_data': cloud_text}
    return json.dumps(time_lis)

def st_variation(lis1, lis2, lis3):
    ave = np.mean(lis1)    
    variation1 = [np.abs(num - ave)/ave for num in lis1]
    ave = np.mean(lis2)
    variation2 = [np.abs(num - ave)/ave for num in lis2]
    ave = np.mean(lis3)
    variation3 = [np.abs(num - ave)/ave for num in lis3]
    variation = [variation1[cursor]+variation2[cursor]+variation3[cursor] for cursor in range(len(lis1))]
    return variation

def find_topN(lis,n):
    new = [lis[0]]
    rank = [0]
    num_cursor = 1
    for num in lis[1:]:
        num_cursor += 1
        find = 0
        cursor = 0
        if num > new[0]:
            new[0:0] = [num]
            rank[0:0] = [num_cursor-1]
        else:
            for i in new:
                if num > i:
                    new[cursor:cursor] = [num]
                    rank[cursor:cursor] = [num_cursor-1]
                    find = 1
                    break
                cursor += 1
            if find == 0:
                new.append(num)
                rank.append(num_cursor-1)
            
    peak_x = []
    peak_y = []
    cursor = 0
    for y in new:
        if rank[cursor]!=0 and rank[cursor]!=len(new)-1:
            if y > lis[rank[cursor]+1] and y > lis[rank[cursor]-1]:
                peak_x.append(rank[cursor])
                peak_y.append(y)

        elif rank[cursor]==0:
            if y > lis[rank[cursor]+1]:
                peak_x.append(rank[cursor])
                peak_y.append(y)
        elif rank[cursor]==rank[cursor]!=len(new)-1:
            if y > lis[rank[cursor]+1]:
                peak_x.append(rank[cursor])
                peak_y.append(y)
        if len(peak_x)==n:
            break
        cursor += 1
    return peak_x[:n],peak_y[:n]