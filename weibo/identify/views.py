# -*- coding: utf-8 -*-

import os
import sys
import time
from datetime import datetime
import subprocess
try:
    import simplejosn as json
except ImportError:
    import json

import weibo.model
from weibo.model import *
from weibo.extensions import db
import json

from flask import Blueprint, url_for, render_template, request, abort, flash, make_response, session, redirect

import whole as wholeModule

import area as areaModule

import burst as burstModule

import burst_monitor as burstRealtimeModule

from show_user_statuses import user_statuses

from utils import acquire_topic_id, acquire_topic_name, read_rank_results, read_topic_rank_results, read_topic_gexf_results
from time_utils import ts2datetime, datetime2ts, window2time

from hadoop_utils import monitor

from weibo.global_config import xapian_search_domain, fields_id, xapian_search_user

from whole_result import whole_caculate
from area_result import area_caculate
from brust_result import burst_caculate
from history import _all_history, _add_history, _search_history

import networkx as nx
from lxml import etree

#from weibo.cron_check import topic_network

domain={'culture': 0, 'education': 1, 'entertainment': 2, 'fashion': 3, 'finance': 4, 'media': 5, 'sports': 6, 'technology': 7, 'oversea': 8, 'university': 9, 'homeadmin': 10, 'abroadadmin': 11, 'homemedia': 12, 'abroadadmin': 13, 'folkorg': 14, 'lawyer': 15, 'politician': 16, 'mediaworker': 17, 'activer': 18, 'grassroot': 19, 'other': 20}

mod = Blueprint('identify', __name__, url_prefix='/identify')

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

def burst2ts(burst_time):

    month_day, year = burst_time.split(',')
    month, day = month_day.split('月 ')
    year = int(year)
    month = int(month)
    day = filter(str.isdigit, day)#只保留数字，去掉“日”
    day = int(day)
    ts = datetime(year, month, day, 0, 0, 0)
    ts = time.mktime(ts.timetuple())

    return int(ts)

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
            return render_template('identify/index.html')
        else:
            pas = db.session.query(UserList).filter(UserList.username==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.identify
                    if identy == 1:
                        return render_template('identify/index.html')
                    else:
                        return redirect('/')
            return redirect('/')
    else:
        return redirect('/')

@mod.route("/whole/", methods=["GET", "POST"])
def whole():
    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin':
            request_method = request.method
            if request_method == 'GET':
                return render_template('identify/whole.html', from_external=True)
            elif request_method == 'POST':
                form = request.form
                action = form.get('action', 'run')

                top_n = int(form.get('top_n', 500))
        
                if top_n > 500:
                    top_n = 500

                page_num = int(form.get('page_num', 20))
                rank_method = form.get('rank_method', 'followers')
                during_date = form.get('window_size', '9月 1日,2013 - 9月 5日,2013')
                during_date = _utf_encode(during_date)
                start_ts,end_ts = _time_zone(during_date)
                window_size = (end_ts - start_ts)/(24*3600)
                
                if action == 'rank':
                    current_date = ts2datetime(end_ts-24*3600)
                    previous_date = ts2datetime(start_ts-24*3600)
                    data = whole_caculate(current_date,window_size,rank_method,top_n)
                    previous_data = whole_caculate(previous_date,window_size,rank_method,top_n)

                    index = dict()
                    for i in range(0,len(data)):#比较上期结果
                        flag = 0
                        for j in range(0,len(previous_data)):
                            if previous_data[j][1] == data[i][1]:
                                flag = 1
                                compare = previous_data[j][0] - data[i][0]
                                index[previous_data[j][1]] = j
                                break
                        if flag == 0:
                            compare = 0
                        data[i].append(compare)

                    pre_data = []
                    for i in range(0,len(previous_data)):
                        if  index.has_key(previous_data[i][1]):
                            pass
                        else:
                            pre_data.append(previous_data[i])

                    return json.dumps({'status': 'current finished', 'data': data, 'pre_data': pre_data})
                elif action == 'run':
                    during_date = _utf_decode(during_date)
                    return render_template('identify/whole.html', rank_method=rank_method, during_date=during_date, top_n=top_n, page_num=page_num)
                else:
                   abort(404)
            else:
                abort(404)
        else:
            pas = db.session.query(UserList).filter(UserList.username==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.identify
                    if identy == 1:
                        request_method = request.method
                        if request_method == 'GET':
                            return render_template('identify/whole.html', from_external=True)
                        elif request_method == 'POST':
                            form = request.form
                            action = form.get('action', 'run')

                            top_n = int(form.get('top_n', 500))
        
                            if top_n > 500:
                                top_n = 500
                            page_num = int(form.get('page_num', 20))
                            rank_method = form.get('rank_method', 'followers')
                            during_date = form.get('window_size', '9月 1日,2013 - 9月 5日,2013')
                            during_date = _utf_encode(during_date)
                            start_ts,end_ts = _time_zone(during_date)
                            window_size = (end_ts - start_ts)/(24*3600)
                
                            if action == 'rank':
                                current_date = ts2datetime(end_ts-24*3600)
                                previous_date = ts2datetime(start_ts-24*3600)
                                data = whole_caculate(current_date,window_size,rank_method,top_n)
                                previous_data = whole_caculate(previous_date,window_size,rank_method,top_n)

                                index = dict()
                                for i in range(0,len(data)):#比较上期结果
                                    flag = 0
                                    for j in range(0,len(previous_data)):
                                        if previous_data[j][1] == data[i][1]:
                                            flag = 1
                                            compare = previous_data[j][0] - data[i][0]
                                            index[previous_data[j][1]] = j
                                            break
                                    if flag == 0:
                                        compare = 0
                                    data[i].append(compare)

                                pre_data = []
                                for i in range(0,len(previous_data)):
                                    if  index.has_key(previous_data[i][1]):
                                        pass
                                    else:
                                        pre_data.append(previous_data[i])

                                return json.dumps({'status': 'current finished', 'data': data, 'pre_data': pre_data})
                            elif action == 'run':
                                during_date = _utf_decode(during_date)
                                return render_template('identify/whole.html', rank_method=rank_method, during_date=during_date, top_n=top_n, page_num=page_num)
                            else:
                               abort(404)
                        else:
                            abort(404)
                else:
                    return redirect('/')
            return redirect('/')
    else:
        return redirect('/')

@mod.route("/burst/", methods=["GET", "POST"])
def burst():
    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin':
            request_method = request.method
            if request_method == 'GET':
                return render_template('identify/burst.html', from_external=True)
            elif request_method == 'POST':
                form = request.form
                action = form.get('action', 'run')

                top_n = int(form.get('top_n', 500))

                if top_n > 500:
                    top_n = 500
                page_num = int(form.get('page_num', 20))
                rank_method = form.get('rank_method', 'active')
                burst_time = form.get('burst_time', '9月 1日,2013')

                burst_time = _utf_encode(burst_time)
                time_ts = burst2ts(burst_time)

                if action == 'rank':
                    current_date = ts2datetime(time_ts)
                    data = burst_caculate(current_date, 1, rank_method, top_n)
                    previous_date = ts2datetime(time_ts-24*3600)
                    previous_data = burst_caculate(previous_date, 1, rank_method, top_n)

                    index = dict()
                    for i in range(0,len(data)):#比较上期结果
                        flag = 0
                        for j in range(0,len(previous_data)):
                            if previous_data[j][1] == data[i][1]:
                                flag = 1
                                compare = previous_data[j][0] - data[i][0]
                                index[previous_data[j][1]] = j
                                break
                        if flag == 0:
                            compare = 0
                        data[i].append(compare)

                    pre_data = []
                    for i in range(0,len(previous_data)):
                        if  index.has_key(previous_data[i][1]):
                            pass
                        else:
                            pre_data.append(previous_data[i])
                    
                    return json.dumps({'status': 'current finished', 'data': data, 'pre_data': pre_data, 'method':rank_method, 'time': time_ts})
                elif action == 'run':
                    burst_time = _utf_decode(burst_time)
                    return render_template('identify/burst.html', rank_method=rank_method, burst_time=burst_time, top_n=top_n, page_num=page_num)
                else:
                   abort(404)
            else:
                abort(404)
        else:
            pas = db.session.query(UserList).filter(UserList.username==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.identify
                    if identy == 1:
                        request_method = request.method
                        if request_method == 'GET':
                            return render_template('identify/burst.html', from_external=True)
                        elif request_method == 'POST':
                            form = request.form
                            action = form.get('action', 'run')

                            top_n = int(form.get('top_n', 500))

                            if top_n > 500:
                                top_n = 500
                            page_num = int(form.get('page_num', 20))
                            rank_method = form.get('rank_method', 'active')
                            burst_time = form.get('burst_time', '9月 1日,2013')

                            burst_time = _utf_encode(burst_time)
                            time_ts = burst2ts(burst_time)

                            if action == 'rank':
                                current_date = ts2datetime(time_ts)
                                data = burst_caculate(current_date, 1, rank_method, top_n)
                                previous_date = ts2datetime(time_ts-24*3600)
                                previous_data = burst_caculate(previous_date, 1, rank_method, top_n)

                                index = dict()
                                for i in range(0,len(data)):#比较上期结果
                                    flag = 0
                                    for j in range(0,len(previous_data)):
                                        if previous_data[j][1] == data[i][1]:
                                            flag = 1
                                            compare = previous_data[j][0] - data[i][0]
                                            index[previous_data[j][1]] = j
                                            break
                                    if flag == 0:
                                        compare = 0
                                    data[i].append(compare)

                                pre_data = []
                                for i in range(0,len(previous_data)):
                                    if  index.has_key(previous_data[i][1]):
                                        pass
                                    else:
                                        pre_data.append(previous_data[i])
                    
                                return json.dumps({'status': 'current finished', 'data': data, 'pre_data': pre_data, 'method':rank_method, 'time': time_ts})
                            elif action == 'run':
                                burst_time = _utf_decode(burst_time)
                                return render_template('identify/burst.html', rank_method=rank_method, burst_time=burst_time, top_n=top_n, page_num=page_num)
                    else:
                        return redirect('/')
            return redirect('/')
    else:
        return redirect('/')

@mod.route("/area/", methods=["GET", "POST"])
def area():
    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin':
            request_method = request.method
            if request_method == 'GET':
                return render_template('identify/area.html', from_external=True, field='university')
            elif request_method == 'POST':
                form = request.form
                action = form.get('action', 'run')
                top_n = int(form.get('top_n', 500))
                if top_n > 500:
                    top_n = 500

                page_num = int(form.get('page_num', 20))
                rank_method = form.get('rank_method', 'followers')
                during_date = form.get('window_size', '9月 1日,2013 - 9月 5日,2013')
                during_date = _utf_encode(during_date)
                start_ts,end_ts = _time_zone(during_date)
                window_size = (end_ts - start_ts)/(24*3600)

                field = form.get('field', 'university')
                field_id = domain[field]

                if action == 'previous_rank':
                    action = 'rank'
                if action == 'rank':
                    current_date = ts2datetime(end_ts-24*3600)
                    previous_date = ts2datetime(start_ts-24*3600)

                    data = area_caculate(current_date,window_size,rank_method,top_n,field_id)
                    previous_data = area_caculate(previous_date,window_size,rank_method,top_n,field_id)

                    index = dict()
                    for i in range(0,len(data)):#比较上期结果
                        flag = 0
                        for j in range(0,len(previous_data)):
                            if previous_data[j][1] == data[i][1]:
                                flag = 1
                                compare = previous_data[j][0] - data[i][0]
                                index[previous_data[j][1]] = j
                                break
                        if flag == 0:
                            compare = 0
                        data[i].append(compare)

                    pre_data = []
                    for i in range(0,len(previous_data)):
                        if  index.has_key(previous_data[i][1]):
                            pass
                        else:
                            pre_data.append(previous_data[i])
                    return json.dumps({'status': 'current finished', 'data': data, 'pre_data': pre_data})
                elif action == 'run':
                    during_date = _utf_decode(during_date)
                    return render_template('identify/area.html', rank_method=rank_method, during_date=during_date, top_n=top_n, page_num=page_num, field=field)
        else:
            return redirect('/')
    else:
        return redirect('/')
    
@mod.route("/topic/", methods=["GET", "POST"])
def topic():
    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin':
            request_method = request.method

            if request_method == 'GET':
                args = request.args
                topic = args.get('keyword', None)
                time = args.get('time', None)
                rank_method = args.get('rank_method', 'pagerank')
                page_num = args.get('page_num', 20)
                top_n = args.get('top_n', 500)

                if not topic or not time:
                    return redirect('/identify/')

                dur_time = _utf_encode(time)
                start_ts, end_ts = _time_zone(dur_time)
                print dur_time
                print start_ts, end_ts

                return render_template('identify/topic.html', topic=topic, start_ts=start_ts, \
                                       end_ts=end_ts, rank_method=rank_method, page_num=page_num, \
                                       top_n=top_n, window_size=1)

            if request_method == 'POST':
                form = request.form
                topic = form.get('topic', None)
                start_ts = form.get('start_ts', None)
                end_ts = form.get('end_ts', None)
                if start_ts:
                    start_ts = int(start_ts)
                if end_ts:
                    end_ts = int(end_ts)
                rank_method = form.get('rank_method', 'pagerank')
                page_num = int(form.get('page_num', 20))
                top_n = int(form.get('top_n', 500))

                current_date = ts2datetime(end_ts)
                window_size = (end_ts - start_ts) / (24 * 3600)
                data = read_topic_rank_results(topic, top_n, rank_method, current_date, window_size)

                return json.dumps({'status': 'current finished', 'data': data})
    else:
        return redirect('/')

@mod.route("/topic/network/", methods=["POST"])
def area_network():
    request_method = request.method
    if request_method == 'POST':
        gexf = None
        form = request.form
        topic = form.get('topic', None)
        start_ts = form.get('start_ts', None)
        end_ts = form.get('end_ts', None)
        window_size = 1

        if start_ts:
            start_ts = int(start_ts)
        if end_ts:
            end_ts = int(end_ts)
        identifyDate = ts2datetime(end_ts)
        window_size = int((end_ts - start_ts) / (24 * 3600))

        gexf = read_topic_gexf_results(topic, identifyDate, window_size)
        if not gexf:
            gexf = ''

        response = make_response(gexf)
        response.headers['Content-Type'] = 'text/xml'
        return response

    else:
        abort(404)


def _utf_encode(s):
    if isinstance(s, str):
        return s
    else:
        return s.encode('utf-8')


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

    return int(start_ts), int(end_ts) + 3600 * 24

@mod.route("/monitor/burst/", methods=["GET", "POST"])
def burst_monitor():
    request_method = request.method
    if request_method == 'POST':
        form = request.form
        current_time = time.time()
        current_date = ts2datetime(current_time)
        data = burst_caculate(current_date, 1, 'active', 5)
        return json.dumps(data)
    else:
        abort(404)

@mod.route("/statuses/<int:uid>/<int:page>/<int:time_ts>")
def show_user_statuses(uid, page, time_ts):
    if 'logged_in' in session and session['logged_in']:
        statuses = user_statuses(uid, page, time_ts)
        return render_template('identify/user_statuses.html', statuses=statuses)
    else:
        return redirect('/')

@mod.route("/add_kd/", methods=['GET','POST'])
def add_kd():
    result = 'Right'
    new_field = request.form['f_id']
    count, get_results = xapian_search_user.search(query={'_id': new_field}, fields=['_id', 'name'])
    if count > 0:
        for get_result in get_results():
            new_item = KnowledgeList(kID=get_result['_id'],kName=get_result['name'])
            db.session.add(new_item)
            db.session.commit()
    else:
        result = 'Wrong'
    return json.dumps(result)

@mod.route("/remove_kd/", methods=['GET','POST'])
def remove_kd():
    result = 'Right'
    new_id = request.form['f_id']
    old_items = db.session.query(KnowledgeList).filter(KnowledgeList.kID==new_id).all()
    if len(old_items):
        for old_item in old_items:
            db.session.delete(old_item)
            db.session.commit()
    else:
        result = 'Wrong'
    return json.dumps(result)

@mod.route("/add_trash/", methods=['GET','POST'])
def add_trash():
    result = 'Right'
    new_field = request.form['f_id']
    count, get_results = xapian_search_user.search(query={'_id': new_field}, fields=['_id', 'name'])
    if count > 0:
        for get_result in get_results():
            new_item = BlackList(blackID=get_result['_id'],blackName=get_result['name'])
            db.session.add(new_item)
            db.session.commit()
    else:
        result = 'Wrong'
    return json.dumps(result)

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
                end = time.strftime("%m月 %d日, %Y", time.localtime(history.end-24*3600))
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
                end = time.strftime("%m月 %d日, %Y", time.localtime(history.end-24*3600))
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
                end = time.strftime("%m月 %d日, %Y", time.localtime(history.end-24*3600))
                datestr  = str(start) + ' - ' + str(end)
                histories_names.append([history.topic, datestr])
        return json.dumps(histories_names)
    else:
        operator = request.form.get('operator', 'add')
        keyword = request.form.get('keyword', '')
        start = request.form.get('start', '')
        end = request.form.get('end', '')
        sentiment = request.form.get('sentiment', '')
        if keyword != '' and start != '' and end != '' and sentiment != '':
            if operator == 'add':
                status, item = _add_history(-1, keyword, start_ts, end_ts, timestamp)
                item = item.topic + '\t' + item.start + '\t' + item.end + '\t' + item.range + '\t' + item.status
            else:
                status, item = 'failed', 'Null'
        else:
            status, item = 'failed', 'Null'
        return json.dumps({'status': status, 'item': item})

@mod.route('/topic/submit', methods=['GET','POST'])
def topic_submit():
    if 'logged_in' in session and session['logged_in']:        
        if session['user'] == 'admin':
            keyword = request.args.get('keyword', None)
            time = request.args.get('time', None)
            timestamp = request.args.get('timestamp', None)
            timestamp = int(timestamp)
            time = _utf_encode(time)
            start_ts, end_ts = _time_zone(time)
            status, item = _add_history(-1, keyword, start_ts, end_ts, timestamp)
            return status, item
            #return render_template('identify/topic.html', topic=keyword, topic_id=1, window_size=1)
    else:
        return redirect('/')

@mod.route('/history/', methods=['GET','POST'])
def history():
    if 'logged_in' in session and session['logged_in']:        
        if session['user'] == 'admin':
            temp_keyword=request.form.get('keyword', None)
            temp_start=request.form.get('start',None)
            temp_end=request.form.get('end',None)
            if temp_keyword:
                return render_template('identify/topic_emotion.html', temp_keyword=temp_keyword)
            else:
                return render_template('identify/topic_emotion.html')
            return render_template('identify/topic_emotion.html') 
    else:
        return redirect('/')
