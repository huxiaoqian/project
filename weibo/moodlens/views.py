# -*- coding: utf-8 -*-


from weibo.model import *
from weibo.extensions import db
from weibo.global_config import xapian_search_weibo, emotions_kv, \
                                xapian_search_domain, LEVELDBPATH
from flask import Blueprint, render_template, request, session, redirect
from utils import getWeiboByMid, st_variation, find_topN, read_range_kcount_results, \
                  sentimentFromDB, sentimentRealTime, read_range_weibos_results
from xapian_weibo.utils import top_keywords
import keywords as keywordsModule
import weibos as weibosModule
import counts as countsModule
from topics import _all_topics, _add_topic, _drop_topic
import simplejson as json
from datetime import date
from datetime import datetime
import time
import leveldb
import os
import weibo.model
import json
import re

month_value = {'January':1, 'February':2, 'March':3, 'April':4, 'May':5, 'June':6, 'July':7, 'August':8, 'September':9, 'October':10, 'November':11, 'December':12}
field_id = {u'文化':'culture', u'教育':'education', u'娱乐':'entertainment', u'时尚':'fashion', u'财经':'finance', u'媒体':'media', u'体育':'sports', u'科技':'technology'}
mod = Blueprint('moodlens', __name__, url_prefix='/moodlens')

buckets = {}
total_days = 90


def get_bucket(bucket):
    if bucket in buckets:
        return buckets[bucket]
    buckets[bucket] = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'lijun_' + bucket),
                                      block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    return buckets[bucket]

def _utf_encode(s):
    if isinstance(s, str):
        return s
    else:
        return s.encode('utf-8')


def _utf_decode(s):
    if isinstance(s, str):
        return s.decode('utf-8')
    else:
        return s

def strToDate(dur_time):
    m = '1'
    d = '1'
    y = '2013'
    items = dur_time.split(', ')
    n = 0
    for item in items:
        if n == 0:
            mds = item.split(' ')
            t = 0
            for md in mds:
                if t==0:
                    m = month_value[md]
                    t = 1
                else:
                    d = md
            n = 1
        else:
            y = item

    time_str = str(y)+'-'+str(m)+'-'+str(d)

    return time_str

def str2ts(s):
    temp_during = _utf_encode(s)
    if re.match(r'\d+分钟', temp_during):
        pattern=re.compile(r'分钟')
        temp_during=int(pattern.split(temp_during)[0])*60
    elif re.match((r'\d+小时'),temp_during):
        pattern=re.compile(r'小时')
        temp_during=int(pattern.split(temp_during)[0])*3600
    elif re.match(r'\d+天',temp_during):
        pattern=re.compile(r'天')
        temp_during=int(pattern.split(temp_during)[0])*24*3600
    return temp_during

@mod.route('/log_in', methods=['GET','POST'])
def log_in():
    session['logged_in'] = request.form['log_in']
    session['user'] = request.form['user']
    if 'logged_in' in session and session['logged_in']:
        return json.dumps('Right')
    else:
        return json.dumps('Wrong')

@mod.route('/', methods=['GET','POST'])
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


@mod.route('/all/', methods=['GET','POST'])
def all_emotion():
    if 'logged_in' in session and session['logged_in']:        
        if session['user'] == 'admin':
            dur_time = request.args.get('time', '')
            during = request.args.get('during', '')
            if dur_time == '':
                dur_day = 5
                during = 15*60
                end_time_day = 4
                end_time_month = 9
                end_time_year = 2013
                return render_template('moodlens/all_emotion.html', active='moodlens',dur_day=dur_day,during=during,end_day=end_time_day,end_month=end_time_month,end_year=end_time_year)
            times = dur_time.split(' - ')
            n = 0
            for ti in times:
                if n==0:
                    beg_time = strToDate(ti)
                    n = 1
                else:
                    end_time = strToDate(ti)

            beg_time = datetime.strptime(beg_time,"%Y-%m-%d")

            end_time = datetime.strptime(end_time,"%Y-%m-%d")
            end_time_year = int(end_time.year)
            end_time_month = int(end_time.month)
            end_time_day = int(end_time.day)

            d1=datetime.date(end_time)
            d2=datetime.date(beg_time)
            dur_day=int((d1-d2).days)

            if during == '':
                during = 15*60
                return render_template('moodlens/all_emotion.html', active='moodlens',dur_day=dur_day,during=during,end_day=end_time_day,end_month=end_time_month,end_year=end_time_year)
            else:
                during = str2ts(during)
                
                return render_template('moodlens/all_emotion.html', active='moodlens',dur_day=dur_day,during=during,end_day=end_time_day,end_month=end_time_month,end_year=end_time_year)
        else:
            pas = db.session.query(UserList).filter(UserList.username==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.moodlens
                    if identy == 1:
                        dur_time = request.args.get('time', '')
                        during = request.args.get('during', '')
            
                        if dur_time == '':
                            dur_day = 5
                            during = 15*60
                            end_time_day = 4
                            end_time_month = 9
                            end_time_year = 2013
                            return render_template('moodlens/all_emotion.html', active='moodlens',dur_day=dur_day,during=during,end_day=end_time_day,end_month=end_time_month,end_year=end_time_year)
                        times = dur_time.split(' - ')
                        n = 0
                        for ti in times:
                            if n==0:
                                beg_time = strToDate(ti)
                                n = 1
                            else:
                                end_time = strToDate(ti)

                        beg_time = datetime.strptime(beg_time,"%Y-%m-%d")

                        end_time = datetime.strptime(end_time,"%Y-%m-%d")
                        end_time_year = int(end_time.year)
                        end_time_month = int(end_time.month)
                        end_time_day = int(end_time.day)

                        d1=datetime.date(end_time)
                        d2=datetime.date(beg_time)
                        dur_day=int((d1-d2).days)

                        if during == '':
                            during = 15*60
                            return render_template('moodlens/all_emotion.html', active='moodlens',dur_day=dur_day,during=during,end_day=end_time_day,end_month=end_time_month,end_year=end_time_year)
                        else:
                            during = str2ts(during)
                            return render_template('moodlens/all_emotion.html', active='moodlens',dur_day=dur_day,during=during,end_day=end_time_day,end_month=end_time_month,end_year=end_time_year)
                    else:
                        return redirect('/')
            return redirect('/')
    else:
        return redirect('/')


@mod.route('/field/', methods=['GET','POST'])
def field():
    if 'logged_in' in session and session['logged_in']:        
        if session['user'] == 'admin':
            dur_time = request.args.get('time', '')
            during = request.args.get('during', '')
            field_name = request.args.get('field_name', '')
            if field_name == '':
                return render_template('moodlens/index.html', active='moodlens')
            else:
                field_en = field_id[field_name]

            if dur_time == '':
                dur_day = 5
                during = 15*60
                end_time_day = 4
                end_time_month = 9
                end_time_year = 2013
                return render_template('moodlens/field_emotion.html', active='moodlens',dur_day=dur_day,during=during,end_day=end_time_day,end_month=end_time_month,end_year=end_time_year,field_en=field_en)
            times = dur_time.split(' - ')
            n = 0
            for ti in times:
                if n==0:
                    beg_time = strToDate(ti)
                    n = 1
                else:
                    end_time = strToDate(ti)

            beg_time = datetime.strptime(beg_time,"%Y-%m-%d")

            end_time = datetime.strptime(end_time,"%Y-%m-%d")
            end_time_year = int(end_time.year)
            end_time_month = int(end_time.month)
            end_time_day = int(end_time.day)

            d1=datetime.date(end_time)
            d2=datetime.date(beg_time)
            dur_day=int((d1-d2).days)

            if during == '':
                during = 15*60
                return render_template('moodlens/field_emotion.html', active='moodlens',dur_day=dur_day,during=during,end_day=end_time_day,end_month=end_time_month,end_year=end_time_year,field_en=field_en)
            else:
                during = str2ts(during)
                
                return render_template('moodlens/field_emotion.html', active='moodlens',dur_day=dur_day,during=during,end_day=end_time_day,end_month=end_time_month,end_year=end_time_year,field_en=field_en)
            #return render_template('moodlens/field_emotion.html', active='moodlens')
        else:
            pas = db.session.query(UserList).filter(UserList.username==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.moodlens
                    if identy == 1:
                        dur_time = request.args.get('time', '')
                        during = request.args.get('during', '')
                        field_name = request.args.get('field_name', '')
                        if field_name == '':
                            return render_template('moodlens/index.html', active='moodlens')
                        else:
                            field_en = field_id[field_name]

                        if dur_time == '':
                            dur_day = 5
                            during = 15*60
                            end_time_day = 4
                            end_time_month = 9
                            end_time_year = 2013
                            return render_template('moodlens/field_emotion.html', active='moodlens',dur_day=dur_day,during=during,end_day=end_time_day,end_month=end_time_month,end_year=end_time_year,field_en=field_en)
                        times = dur_time.split(' - ')
                        n = 0
                        for ti in times:
                            if n==0:
                                beg_time = strToDate(ti)
                                n = 1
                            else:
                                end_time = strToDate(ti)

                        beg_time = datetime.strptime(beg_time,"%Y-%m-%d")

                        end_time = datetime.strptime(end_time,"%Y-%m-%d")
                        end_time_year = int(end_time.year)
                        end_time_month = int(end_time.month)
                        end_time_day = int(end_time.day)

                        d1=datetime.date(end_time)
                        d2=datetime.date(beg_time)
                        dur_day=int((d1-d2).days)

                        if during == '':
                            during = 15*60
                            return render_template('moodlens/field_emotion.html', active='moodlens',dur_day=dur_day,during=during,end_day=end_time_day,end_month=end_time_month,end_year=end_time_year,field_en=field_en)
                        else:
                            during = str2ts(during)
                
                            return render_template('moodlens/field_emotion.html', active='moodlens',dur_day=dur_day,during=during,end_day=end_time_day,end_month=end_time_month,end_year=end_time_year,field_en=field_en)
                    else:
                        return redirect('/')
            return redirect('/')
    else:
        return redirect('/')

@mod.route('/topic/', methods=['GET','POST'])
def topic():
    if 'logged_in' in session and session['logged_in']:        
        if session['user'] == 'admin':
            temp_keyword=request.form.get('keyword', None)
            temp_during=request.form.get('during', 24*3600)
            if not isinstance(temp_during, int):
                temp_during = str2ts(temp_during)
            if temp_keyword:
                return render_template('moodlens/topic_emotion.html', active='moodlens',temp_keyword=temp_keyword, temp_during=temp_during)
            else:
                return render_template('moodlens/topic_emotion.html', active='moodlens')
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


@mod.route('/data/<area>/', methods=['GET','POST'])
def data(area='global'):
    """分类情感数据
    """

    query = request.args.get('query', None)
    if query:
        query = query.strip()
    during = request.args.get('during', 24*3600)
    during = int(during)
    print during
    ts = request.args.get('ts', '')
    ts = long(ts)
    begin_ts = ts - during
    end_ts = ts
    emotion = request.args.get('emotion', 'global')

    results = {}

    if area == 'global':
        search_method = 'global'
        if query:
            search_method = 'topic'
        area = None
    else:
        search_method = 'domain'
        
    search_func = getattr(countsModule, 'search_%s_counts' % search_method, None)

    if search_func:
        if emotion == 'global':
            for k, v in emotions_kv.iteritems():
                results[k] = search_func(end_ts, during, v, query=query, domain=area)
        else:
            results[emotion] = search_func(end_ts, during, emotions_kv[emotion], query=query, domain=area)
    
    else:
        return json.dumps('search function undefined')

    return json.dumps(results)

    
@mod.route('/field_data/<area>/')
def field_data(area):
    """/keywords_data 接口已备好，只是差领域数据
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
    """情绪关键词数据
    """

    query = request.args.get('query', None)
    if query:
        query = query.strip()
    during = request.args.get('during', 24*3600)
    during = int(during)
    print during
    ts = request.args.get('ts', '')
    ts = long(ts)
    begin_ts = ts - during
    end_ts = ts
    limit = request.args.get('limit', 50)
    limit = int(limit)
    emotion = request.args.get('emotion', 'global')

    results = {}

    if area == 'global':
        search_method = 'global'
        if query:
            search_method = 'topic'
        area = None
    else:
        search_method = 'domain'
        
    search_func = getattr(keywordsModule, 'search_%s_keywords' % search_method, None)
    print search_func

    if search_func:
        if emotion == 'global':
            for k, v in emotions_kv.iteritems():
                results[k] = search_func(end_ts, during, v, query=query, domain=area, top=limit)
        else:
            results[emotion] = search_func(end_ts, during, emotions_kv[emotion], query=query, domain=area, top=limit)
    
    else:
        return json.dumps('search function undefined')

    return json.dumps(results)


@mod.route('/weibos_data/<emotion>/<area>/')
def weibos_data(emotion='global', area='global'):
    """关键微博
    """

    query = request.args.get('query', None)
    if query:
        query = query.strip()
    during = request.args.get('during', 24*3600)
    during = int(during)
    print during
    ts = request.args.get('ts', '')
    ts = long(ts)
    begin_ts = ts - during
    end_ts = ts
    limit = request.args.get('limit', 50)
    limit = int(limit)

    results = {}

    if area == 'global':
        search_method = 'global'
        if query:
            search_method = 'topic'
        area = None
    else:
        search_method = 'domain'
        
    search_func = getattr(weibosModule, 'search_%s_weibos' % search_method, None)

    if search_func:
        if emotion == 'global':
            for k, v in emotions_kv.iteritems():
                results[k] = search_func(end_ts, during, v, query=query, domain=area, top=limit)
        else:
            results[emotion] = search_func(end_ts, during, emotions_kv[emotion], query=query, domain=area, top=limit)
    
    else:
        return json.dumps('search function undefined')

    print results

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

@mod.route('/topics.json', methods=['GET','POST'])
def topics_customized():
    if request.method == 'GET':

        topics = _all_topics(True)
        topics_names = []

        for topic in topics:
            topics_names.append(topic.topic)

        return json.dumps(topics_names)

    else:
        operator = request.form.get('operator', 'add')
        topic = request.form.get('topic', '')

        if topic != '':
            if operator == 'add':
                status, item = _add_topic(topic)
            if operator == 'del':
                status, item = _drop_topic(topic)

            item = item.topic
        else:
            status, item = 'NoTopic', 'Null'

        return json.dumps({'status': status, 'item': item})

