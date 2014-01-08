# -*- coding: utf-8 -*-


from weibo.model import *
from weibo.extensions import db
from weibo.global_config import xapian_search_weibo, emotions_kv, \
                                xapian_search_domain, LEVELDBPATH
from flask import Blueprint, render_template, request, session, redirect
from utils import getWeiboByMid, weiboinfo2url
from peak_detection import detect_peaks
from xapian_weibo.utils import top_keywords
import keywords as keywordsModule
import weibos as weibosModule
import counts as countsModule
from topics import _all_topics, _add_topic, _drop_topic, _search_topic
from history import _all_history, _add_history, _search_history
import simplejson as json
from datetime import date
from datetime import datetime
import time
import leveldb
import os
import weibo.model
import json
import re
from datetime import datetime


mod = Blueprint('moodlens', __name__, url_prefix='/moodlens')

#month_value = {'January':1, 'February':2, 'March':3, 'April':4, 'May':5, 'June':6, 'July':7, 'August':8, 'September':9, 'October':10, 'November':11, 'December':12}
field_id = {u'文化':'culture', u'教育':'education', u'娱乐':'entertainment', u'时尚':'fashion', u'财经':'finance', u'媒体':'media', u'体育':'sports', u'科技':'technology'}
FIELDS_VALUE = ['culture', 'education', 'entertainment', 'fashion', 'finance', 'media', 'sports', 'technology', 'oversea']
FIELDS_ZH_NAME = [u'文化', u'教育', u'娱乐', u'时尚', u'财经', u'媒体', u'体育', u'科技', u'海外']
FIELDS2ID = {}
FIELDS2ZHNAME = {}

Minute = 60
Fifteenminutes = 15 * Minute
Hour = 3600
SixHour = Hour * 6
Day = Hour * 24
MinInterval = Fifteenminutes

for key in FIELDS_VALUE:
    idx = FIELDS_VALUE.index(key)
    FIELDS2ID[key] = idx
    FIELDS2ZHNAME[key] = FIELDS_ZH_NAME[idx]

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


def str2ts(s):
    '''
    返回以秒为单位的时间间隔
    '''
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

def _default_time_zone():
    '''默认时间段为最新一周
    '''

    end_ts = time.time()
    start_ts = end_ts - 7 * 24 * 3600

    return start_ts, end_ts


def _time_zone(stri):
    '''时间段参数从前台时间控件传来
    '''
    dates = stri.split(' - ')
    tslist = []

    for date in dates:
        month_day, year = date.split(',')
        month, day = month_day.split('月 ')
        year = int(year)
        month = int(month)
        day = filter(str.isdigit, day)#只保留数字，去掉“日”
        day = int(day)
        ts = datetime(year, month, day, 0, 0, 0)
        ts = time.mktime(ts.timetuple())
        tslist.append(ts)

    start_ts = tslist[0]
    end_ts = tslist[1]

    return int(start_ts), int(end_ts)


@mod.route('/all/', methods=['GET','POST'])
def all_emotion():
    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin':
            during = request.args.get('during', None)
            if not during or during == '':
                during = MinInterval
            else:
                during = str2ts(during)
            
            dur_time = request.args.get('time', None)
            dur_time = _utf_encode(dur_time)
            if not dur_time or dur_time == '':
                start_ts, end_ts = _default_time_zone()
            else:
                start_ts, end_ts = _time_zone(dur_time)

            return render_template('moodlens/all_emotion.html', start_ts=start_ts, end_ts=end_ts, during=during)
            
        else:
            pass

    else:
        return redirect('/')


@mod.route('/field/', methods=['GET','POST'])
def field():
    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin':
            during = request.args.get('during', None)
            if not during or during == '':
                during = MinInterval
            else:
                during = str2ts(during)
            
            dur_time = request.args.get('time', None)
            dur_time = _utf_encode(dur_time)
            if not dur_time or dur_time == '':
                start_ts, end_ts = _default_time_zone()
            else:
                start_ts, end_ts = _time_zone(dur_time)

            field_name = request.args.get('field_name', '')
            if field_name == '':
                return render_template('moodlens/index.html', active='moodlens')
            else:
                field_en = field_id[field_name]

            return render_template('moodlens/field_emotion.html', start_ts=start_ts, end_ts=end_ts, during=during,field_en=field_en)
            
        else:
            pass

    else:
        return redirect('/')

@mod.route('/topic/', methods=['GET','POST'])
def topic():
    if 'logged_in' in session and session['logged_in']:        
        if session['user'] == 'admin':
            customized = request.args.get('customized', '1')
 
            during = request.args.get('during', None)
            if not during or during == '':
                during = MinInterval
            else:
                during = str2ts(during)
            
            dur_time = request.args.get('time', None)
            dur_time = _utf_encode(dur_time)
            if not dur_time or dur_time == '':
                start_ts, end_ts = _default_time_zone()
            else:
                start_ts, end_ts = _time_zone(dur_time)

            keyword = request.args.get('keyword', None)

            if keyword and keyword != '':
                keyword = _utf_decode(keyword)
                return render_template('moodlens/topic_emotion.html', keyword=keyword, start_ts=start_ts, end_ts=end_ts, during=during, customized=customized)
            else:
                return render_template('moodlens/index.html')

        else:
            pass
    else:
        return redirect('/')


@mod.route('/data/<area>/', methods=['GET','POST'])
def data(area='global'):
    """分类情感数据
    """
    
    customized = request.args.get('customized', '1')
    print customized
    query = request.args.get('query', None)
    if query:
        query = query.strip()
    during = request.args.get('during', 24*3600)
    during = int(during)
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
        area = FIELDS2ID[area]
   
    search_func = getattr(countsModule, 'search_%s_counts' % search_method, None)
    
    if search_func:
        if emotion == 'global':
            for k, v in emotions_kv.iteritems():
                results[k] = search_func(end_ts, during, v, query=query, domain=area, customized=customized)
        else:
            results[emotion] = search_func(end_ts, during, emotions_kv[emotion], query=query, domain=area, customized=customized)
    else:
        return json.dumps('search function undefined')

    return json.dumps(results)


@mod.route('/keywords_data/<area>/')
def keywords_data(area='global'):
    """情绪关键词数据
    """
    
    customized = request.args.get('customized', '1')
    query = request.args.get('query', None)
    if query:
        query = query.strip()
    during = request.args.get('during', 24*3600)
    during = int(during)

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
        #print "here :"+area
        area = FIELDS2ID[area]
        
    search_func = getattr(keywordsModule, 'search_%s_keywords' % search_method, None)

    if search_func:
        if emotion == 'global':
            for k, v in emotions_kv.iteritems():
                results[k] = search_func(end_ts, during, v, query=query, domain=area, top=limit, customized=customized)
        else:
            results[emotion] = search_func(end_ts, during, emotions_kv[emotion], query=query, domain=area, top=limit, customized=customized)
    
    else:
        return json.dumps('search function undefined')

    return json.dumps(results)

@mod.route('/weibos_data/<emotion>/<area>/')
def weibos_data(emotion='global', area='global'):
    """关键微博
    """
    
    customized = request.args.get('customized', '1')
    query = request.args.get('query', None)
    if query:
        query = query.strip()
    during = request.args.get('during', 24*3600)
    during = int(during)

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
        area = FIELDS2ID[area]
        
    search_func = getattr(weibosModule, 'search_%s_weibos' % search_method, None)
    
    if search_func:
        if emotion == 'global':
            for k, v in emotions_kv.iteritems():
                results[k] = search_func(end_ts, during, v, query=query, domain=area, top=limit, customized=customized)
        else:
            results[emotion] = search_func(end_ts, during, emotions_kv[emotion], query=query, domain=area, top=limit, customized=customized)
    
    else:
        return json.dumps('search function undefined')

    return json.dumps(results)


@mod.route('/emotionpeak/')
def getPeaks():
    '''获取情绪拐点数据
    '''

    customized = request.args.get('customized', '1')
    limit = request.args.get('limit', 10)
    query = request.args.get('query', None)
    if query:
        query = query.strip()
    during = request.args.get('during', 24 * 3600)
    during = int(during)
    area = request.args.get('area', 'global')
    emotion = request.args.get('emotion', 'happy')
    lis = request.args.get('lis', '')

    try:
        lis = [float(da) for da in lis.split(',')]
    except:
        lis = []
    if not lis or not len(lis):
        return 'Null Data'

    ts_lis = request.args.get('ts', '')
    ts_lis = [float(da) for da in ts_lis.split(',')]

    new_zeros = detect_peaks(lis)

    if area == 'global':
        search_method = 'global'
        if query:
            search_method = 'topic'
        area = None
    else:
        search_method = 'domain'
        
    search_func = getattr(keywordsModule, 'search_%s_keywords' % search_method, None)

    if not search_func:
        return json.dumps('search function undefined')

    title_text = {'happy': [], 'angry': [], 'sad': []}
    title = {'happy': 'A', 'angry': 'B', 'sad': 'C'}

    time_lis = {}
    for i in range(0, len(ts_lis)):
        if i in new_zeros:
            ts = ts_lis[i]
            begin_ts = ts - during
            end_ts = ts

            v = emotions_kv[emotion]
            keywords_with_count = search_func(end_ts, during, v, query=query, domain=area, top=limit, customized=customized)
            text = ','.join([k for k, v in keywords_with_count.iteritems()])

            time_lis[i] = {
                'ts': end_ts * 1000,
                'title': title[emotion] + str(new_zeros.index(i)),
                'text': text
            }
        
    return json.dumps(time_lis)

@mod.route('/topic/submit', methods=['GET','POST'])
def topic_submit():
    if 'logged_in' in session and session['logged_in']:        
        if session['user'] == 'admin':
            keyword = request.args.get('keyword', None)
            during = request.args.get('during', None)
            time = request.args.get('time', None)
            timestamp = request.args.get('timestamp', None)
            timestamp = int(timestamp)
            time = _utf_encode(time)
            start_ts, end_ts = _time_zone(time)
            if not during or during == '':
                during = MinInterval
            else:
                during = str2ts(during)
            status , item = _add_history(-1, keyword, start_ts, end_ts, timestamp, during)
            return render_template('moodlens/topic_emotion.html', active='moodlens', temp_keyword=keyword, temp_during=during)
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

@mod.route('/history/', methods=['GET','POST'])
def history():
    if 'logged_in' in session and session['logged_in']:        
        if session['user'] == 'admin':
            temp_keyword=request.form.get('keyword', None)
            temp_during=request.form.get('during', 24*3600)
            temp_start=request.form.get('start',None)
            temp_end=request.form.get('end',None)
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

@mod.route('/topics.json', methods=['GET','POST'])
def topics_customized():
    if request.method == 'GET':
        keyword = request.args.get('keyword',None)
        if keyword:
            topics = _search_topic(keyword,True)
        else:
            topics = _all_topics(True)
        topics_names = []
        if topics:
            for topic in topics:
                db_date = time.strftime("%m月 %d日, %Y %H:%M:%S", time.localtime(topic.db_date))
                datestr = '     提交时间: ' + db_date
                # topics_names.append(topic.topic)
                topics_names.append([topic.topic, datestr])
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

@mod.route('/history.json', methods=['GET','POST'])
def search_history():
    if request.method == 'GET':
        keyword = request.args.get('keyword',None)
        now1 = request.args.get('now1', None)
        now2 = request.args.get('now2', None)
        now = request.args.get('now', None)
        timestamp_end = request.args.get('timestamp', None)
        if timestamp_end:
            timestamp_end = int(timestamp_end)
        if now1:
            now1 = int(now1)
        if now2:
            now2 = int(now2)
        if now:
            now = int(now)
        histories1 = None
        histories2 = None
        histories = None
        if keyword != None:
            status, histories = _search_history(keyword)
        else:
            if now:
                status, histories = _all_history(now)
            if now1:
                status, histories1 = _all_history(now1)
            if now2 == 0:
                status, histories2 = _all_history(now2)
        histories_names = []
        if histories1:
            for history in histories1:
                start = time.strftime("%m月 %d日, %Y", time.localtime(history.start))
                end = time.strftime("%m月 %d日, %Y", time.localtime(history.end))
                datestr  = str(start) + ' - ' + str(end)
                if(timestamp_end):
                    timestamp_start = int(history.db_date)
                    time_pass = timestamp_end - timestamp_start
                    time_pass = time.strftime("%M分钟 %S秒 ", time.localtime(time_pass))
                    time_pass = '       已计算时长： ' + str(time_pass)
                    db_date = time.strftime("%m月 %d日, %Y %H:%M:%S", time.localtime(history.db_date))
                    db_date = '     提交时间： ' + str(db_date)
                    histories_names.append([history.topic, datestr, db_date, time_pass ])
                else:
                    histories_names.append([history.topic, datestr])
        if histories2:
            for history in histories2:
                start = time.strftime("%m月 %d日, %Y", time.localtime(history.start))
                end = time.strftime("%m月 %d日, %Y", time.localtime(history.end))
                datestr  = str(start) + ' - ' + str(end)
                if(timestamp_end):
                    timestamp_start = int(history.db_date)
                    time_pass = timestamp_end - timestamp_start
                    time_pass = time.strftime("%M分钟 %S秒 ", time.localtime(time_pass))
                    time_pass = '       已计算时长： ' + str(time_pass)
                    db_date = time.strftime("%m月 %d日, %Y %H:%M:%S", time.localtime(history.db_date))
                    db_date = '     提交时间： ' + str(db_date)
                    histories_names.append([history.topic, datestr, db_date, time_pass ])
                else:
                    histories_names.append([history.topic, datestr])                
        if histories:
            for history in histories:
                start = time.strftime("%m月 %d日, %Y", time.localtime(history.start))
                end = time.strftime("%m月 %d日, %Y", time.localtime(history.end))
                datestr  = str(start) + ' - ' + str(end)
                histories_names.append([history.topic, datestr])
        return json.dumps(histories_names)
    else:
        operator = request.form.get('operator', 'add')
        keyword = request.form.get('keyword', '')
        start = request.form.get('start', '')
        end = request.form.get('end', '')
        range = request.form.get('during', '')
        sentiment = request.form.get('sentiment', '')
        if keyword != '' and start != '' and end != '' and range != '' and sentiment != '':
            if operator == 'add':
                status, item = _add_history(-1, keyword, start, end, range)
                item = item.topic + '\t' + item.start + '\t' + item.end + '\t' + item.range + '\t' + item.status
            else:
                status, item = 'failed', 'Null'
        else:
            status, item = 'failed', 'Null'
        return json.dumps({'status': status, 'item': item})