# -*- coding: utf-8 -*-



from weibo.model import *
from weibo.extensions import db
from weibo.global_config import xapian_search_weibo, emotions_kv, \
                                xapian_search_domain, LEVELDBPATH
from flask import Blueprint, render_template, request, session, redirect
from utils import getWeiboByMid, st_variation, find_topN, read_range_count_results, \
                  read_range_kcount_results, read_range_weibos_results
from xapian_weibo.utils import top_keywords
import simplejson as json
import datetime
import time
import leveldb
import os
import weibo.model
import json

mod = Blueprint('moodlens', __name__, url_prefix='/moodlens')

buckets = {}
total_days = 90


def get_bucket(bucket):
    if bucket in buckets:
        return buckets[bucket]
    buckets[bucket] = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'lijun_' + bucket),
                                      block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    return buckets[bucket]

@mod.route('/log_in', methods=['GET','POST'])
def log_in():
    session['logged_in'] = request.form['log_in']
    session['user'] = request.form['user']
    if 'logged_in' in session and session['logged_in']:
        return json.dumps('Right')
    else:
        return json.dumps('Wrong')

@mod.route('/')
def index():
    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin':
            return render_template('moodlens/index.html', active='moodlens')
        else:
            pas = db.session.query(UserList).filter(UserList.username==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.moodlens
                    if identy == 1:
                        return render_template('moodlens/index.html', active='moodlens')
                    else:
                        return redirect('/')
            return redirect('/')
    else:
        return redirect('/')


@mod.route('/field')
def field():
    if 'logged_in' in session and session['logged_in']:        
        if session['user'] == 'admin':
            return render_template('moodlens/field_emotion.html', active='moodlens')
        else:
            pas = db.session.query(UserList).filter(UserList.username==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.moodlens
                    if identy == 1:
                        return render_template('moodlens/field_emotion.html', active='moodlens')
                    else:
                        return redirect('/')
            return redirect('/')
    else:
        return redirect('/')


@mod.route('/topic')
def topic():
    if 'logged_in' in session and session['logged_in']:        
        if session['user'] == 'admin':
            return render_template('moodlens/topic_emotion.html', active='moodlens')
        else:
            pas = db.session.query(UserList).filter(UserList.username==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.moodlens
                    if identy == 1:
                        return render_template('moodlens/topic_emotion.html', active='moodlens')
                    else:
                        return redirect('/')
            return redirect('/')
    else:
        return redirect('/')


@mod.route('/data/<area>/')
def data(area='global'):
    """
    分类情感数据
    """

    query = request.args.get('query', '')
    query = query.strip()
    ts = request.args.get('ts', '')
    ts = long(ts)
    during = request.args.get('during', 24*3600)
    during = int(during)
    
    begin_ts = ts - during
    end_ts = ts
    results = read_range_count_results(begin_ts, end_ts, during)
    
    return json.dumps(results)


@mod.route('/field_data/<area>/')
def field_data(area):
    """
    /keywords_data 接口已备好，只是差领域数据
    """

    ts = request.args.get('ts', '')
    ts = long(ts)
    during = request.args.get('during', 24*3600)
    during = int(during)

    begin_ts = ts - during
    end_ts = ts
    print begin_ts, end_ts

    emotions_data = {}
    count, field_users = xapian_search_domain.search(query={'domain':str(area)}, sort_by=['-followers_count'], fields=['_id'], max_offset=10000)
    
    query_dict = {
        'timestamp': {'$gt': begin_ts, '$lt': end_ts},
        '$or': [],
    }
    if count:
        for user in field_users():
            query_dict['$or'].append({'user': user['_id']})
    for k, v in emotions_kv.iteritems():
        query_dict['sentiment'] = v
        count = xapian_search_weibo.search(query=query_dict, count_only=True)
        emotions_data[k] = [end_ts * 1000, count] 

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
        count, get_results = xapian_search_weibo.search(query=query_dict, fields=['terms'])
        print count
        keywords_with_count = top_keywords(get_results, top=10)
        text = ','.join([tp[0] for tp in keywords_with_count])
        data.append({
            'x': end_ts * 1000,
            'title': chr(ord('A') + timestamps.index(end_ts)),
            'text': text
        })

    return json.dumps(data)

@mod.route('/keywords_data/<area>/')
def keywords_data(area='global'):
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
    results = read_range_kcount_results(begin_ts, end_ts, during)
    
    return json.dumps(results)


@mod.route('/field_keywords_data/<area>/')
def field_keywords_data(area):
    """
    /keywords_data 接口已备好，只是差领域数据
    """
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

    count, field_users = xapian_search_domain.search(query={'domain':str(area)}, sort_by=['-followers_count'], fields=['_id'], max_offset=10000)
    if count:
        query_dict['$or'] = []
        for user in field_users():
            query_dict['$or'].append({'user': user['_id']})

    count, get_results = xapian_search_weibo.search(query=query_dict, max_offset=100000, sort_by=['-reposts_count'], fields=['terms'])
    keywords_with_count = top_keywords(get_results, top=50)
    keywords_with_count = [list(i) for i in keywords_with_count]

    return json.dumps(keywords_with_count)


@mod.route('/weibos_data/<area>/')
def weibos_data(area='global'):
    """关键词
    """
    query = request.args.get('query', '')
    query = query.strip()
    ts = request.args.get('ts', '')
    ts = long(ts)
    during = request.args.get('during', 24*3600)
    during = int(during)
    
    begin_ts = ts - during
    end_ts = ts
    results = read_range_weibos_results(begin_ts, end_ts, during)
    
    return json.dumps(results)


@mod.route('/field_weibos_data/<emotion>/<area>/')
def field_weibos_data(emotion, area):
    """
    此接口差领域数据，并且还跟另外的接口差领域数据检索途径不大一样
    """
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

    count, field_users = xapian_search_domain.search(query={'domain':str(area)}, sort_by=['-followers_count'], fields=['_id'], max_offset=10000)
    if count:
        query_dict['$or'] = []
        for user in field_users():
            query_dict['$or'].append({'user': user['_id']})

    count, get_results = xapian_search_weibo.search(query=query_dict, max_offset=10, sort_by=['-reposts_count'], fields=['_id'])
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

@mod.route('/emotionpeak/')
def getPeaks():
    happy_lis = request.args.get('happy', '')
    angry_lis = request.args.get('angry', '')
    sad_lis = request.args.get('sad', '')
    ts_lis = request.args.get('ts', '')
    query = request.args.get('query', '')
    query = query.strip()
    
    if not happy_lis or not angry_lis or not sad_lis:
        return 'Null Data'
    happy_lis = [float(da) for da in happy_lis.split(',')]
    angry_lis = [float(da) for da in angry_lis.split(',')]
    sad_lis = [float(da) for da in sad_lis.split(',')]
    ts_lis = [float(da) for da in ts_lis.split(',')]
    topN = 10
    sentiment_variation = st_variation(happy_lis, angry_lis, sad_lis)
    ##peak_x返回前N个点的在list中的序数0,1.
    ##peak_y返回前N个点的情绪波动值
    try:
        peak_x, peak_y = find_topN(sentiment_variation,topN)
    except:
        return 'Null Data'
    sorted_peak_x = sorted(peak_x)
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
            count, get_results = xapian_search_weibo.search(query=query_dict, fields=['terms', 'text', 'user'])
            keywords_with_10count = top_keywords(get_results, top=10)
            title_text[emotion] = ','.join([tp[0] for tp in keywords_with_10count])
            title[emotion] = title[emotion] + str(sorted_peak_x.index(i))

        time_lis[i] = {
            'ts': end_ts * 1000,
            'title': title,
            'text': title_text
        }

    return json.dumps(time_lis)
