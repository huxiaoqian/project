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

from utils import acquire_topic_id, acquire_topic_name, read_rank_results
from time_utils import ts2datetime, datetime2ts, window2time

from hadoop_utils import monitor

from weibo.global_config import xapian_search_domain, fields_id

from whole_result import whole_caculate
from area_result import area_caculate
from brust_result import burst_caculate

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

                    for i in range(0,len(data)):#比较上期结果
                        flag = 0
                        for j in range(0,len(previous_data)):
                            if previous_data[j][1] == data[i][1]:
                                flag = 1
                                compare = previous_data[j][0] - data[i][0]
                                break
                        if flag == 0:
                            compare = 0
                        data[i].append(compare)

                    return json.dumps({'status': 'current finished', 'data': data, 'pre_data': previous_data})
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

                                for i in range(0,len(data)):#比较上期结果
                                    flag = 0
                                    for j in range(0,len(previous_data)):
                                        if previous_data[j][1] == data[i][1]:
                                            flag = 1
                                            compare = previous_data[j][0] - data[i][0]
                                            break
                                    if flag == 0:
                                        compare = 0
                                    data[i].append(compare)

                                return json.dumps({'status': 'current finished', 'data': data, 'pre_data': previous_data})
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

                    for i in range(0,len(data)):#比较上期结果
                        flag = 0
                        for j in range(0,len(previous_data)):
                            if previous_data[j][1] == data[i][1]:
                                flag = 1
                                compare = previous_data[j][0] - data[i][0]
                                break
                        if flag == 0:
                            compare = 0
                        data[i].append(compare)
                    
                    return json.dumps({'status': 'current finished', 'data': data, 'pre_data': previous_data, 'method':rank_method, 'time': time_ts})
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

                                for i in range(0,len(data)):#比较上期结果
                                    flag = 0
                                    for j in range(0,len(previous_data)):
                                        if previous_data[j][1] == data[i][1]:
                                            flag = 1
                                            compare = previous_data[j][0] - data[i][0]
                                            break
                                    if flag == 0:
                                        compare = 0
                                    data[i].append(compare)
                    
                                return json.dumps({'status': 'current finished', 'data': data, 'pre_data': previous_data, 'method':rank_method, 'time': time_ts})
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
                return render_template('identify/area.html', from_external=True)
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

                field = form.get('field', 'finance')
                field_id = domain[field]

                if action == 'previous_rank':
                    action = 'rank'
                if action == 'rank':
                    current_date = ts2datetime(end_ts-24*3600)
                    previous_date = ts2datetime(start_ts-24*3600)

                    data = area_caculate(current_date,window_size,rank_method,top_n,field_id)
                    previous_data = area_caculate(previous_date,window_size,rank_method,top_n,field_id)

                    for i in range(0,len(data)):#比较上期结果
                        flag = 0
                        for j in range(0,len(previous_data)):
                            if previous_data[j][1] == data[i][1]:
                                flag = 1
                                compare = previous_data[j][0] - data[i][0]
                                break
                        if flag == 0:
                            compare = 0
                        data[i].append(compare)
                    return json.dumps({'status': 'current finished', 'data': data, 'pre_data': previous_data})
                elif action == 'run':
                    during_date = _utf_decode(during_date)
                    return render_template('identify/area.html', rank_method=rank_method, during_date=during_date, top_n=top_n, page_num=page_num, field=field)
        else:
            pas = db.session.query(UserList).filter(UserList.username==session['user']).all()
            if pas != []:
                for pa in pas:
                    identy = pa.identify
                    if identy == 1:
                        request_method = request.method
                        if request_method == 'GET':
                            return render_template('identify/area.html', from_external=True)
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

                            field = form.get('field', 'finance')
                            field_id = domain[field]

                            if action == 'previous_rank':
                                action = 'rank'
                            if action == 'rank':
                                current_date = ts2datetime(end_ts-24*3600)
                                previous_date = ts2datetime(start_ts-24*3600)

                                data = area_caculate(current_date,window_size,rank_method,top_n,field_id)
                                previous_data = area_caculate(previous_date,window_size,rank_method,top_n,field_id)

                                for i in range(0,len(data)):#比较上期结果
                                    flag = 0
                                    for j in range(0,len(previous_data)):
                                        if previous_data[j][1] == data[i][1]:
                                            flag = 1
                                            compare = previous_data[j][0] - data[i][0]
                                            break
                                    if flag == 0:
                                        compare = 0
                                    data[i].append(compare)
                                return json.dumps({'status': 'current finished', 'data': data, 'pre_data': previous_data})
                            elif action == 'run':
                                during_date = _utf_decode(during_date)
                                return render_template('identify/area.html', rank_method=rank_method, during_date=during_date, top_n=top_n, page_num=page_num, field=field)
            return redirect('/')
    else:
        return redirect('/')
    
@mod.route("/topic/", methods=["GET", "POST"])
def topic():
    if 'logged_in' in session and session['logged_in']:
        if session['user'] == 'admin':
            request_method = request.method
            if request_method == 'GET':
                return render_template('identify/topic.html', from_external=True)
            elif request_method == 'POST':
        #form data
                form = request.form

        #follow actions: rank|previous_rank|check_status
                action = form.get('action', None)
        #field
                field = form.get('field', None)
        #topic or sub-field
                topic = form.get('topic', None)
                topic_id = form.get('topic_id', None)
        #total results count
                top_n = int(form.get('top_n', 2000))
        #results count for every page
                page_num = int(form.get('page_num', 20))
        #window size for idenfity i.e. adding time limit on input data
                window_size = int(form.get('window_size', 1))
        #use PageRank if and only if window size equals 1
                if window_size == 1 or window_size == 7:
                    rank_method = 'pagerank'
                else:
                    rank_method = 'degree'

        #acquire topic id
                if not topic_id:
                    if topic:
                        topic_id = acquire_topic_id(topic)
                    else:
                        if action == 'run':
                            flash(u'请输入关键词！')
                            return render_template('identify/topic.html', from_external=True)
                        else:
                            return json.dumps({'error': 'need a topic'})

        # current_time = time.time()
                current_time = datetime2ts('2013-3-7')
                if action == 'rank':
                    current_date = ts2datetime(current_time)
                    current_data = read_rank_results(top_n, 'topic', rank_method, current_date, window_size, topic_id=topic_id, compare=True)
                    if not current_data:
                        if rank_method == 'pagerank':
                            rank_func = getattr(areaModule, '%s_rank' % rank_method, None)
                            if rank_func:
                                current_data = rank_func(top_n, current_date, topic_id, window_size)
                        elif rank_method == 'degree':
                            rank_func = getattr(areaModule, '%s_rank' % rank_method, None)
                            if rank_func:
                                current_data = rank_func(top_n, current_date, topic_id, window_size)
                    return json.dumps({'status': 'current finished', 'data': current_data})

                elif action == 'previous_rank':
                    previous_date = ts2datetime(current_time-window2time(window_size))
                    previous_data = read_rank_results(top_n, 'topic', rank_method, previous_date, window_size, topic_id=topic_id)
                    if not previous_data and window_size <= 7:
                        if rank_method == 'pagerank':
                            rank_func = getattr(areaModule, '%s_rank' % rank_method, None)
                            if rank_func:
                                previous_data = rank_func(top_n, previous_date, topic_id, window_size)
                        elif rank_method == 'degree':
                            rank_func = getattr(areaModule, '%s_rank' % rank_method, None)
                            if rank_func:
                                previous_data = rank_func(top_n, previous_date, topic_id, window_size)
                    return json.dumps({'status': 'previous finished', 'data': previous_data})

                elif action == 'check_rank_status':
            #check Hadoop Job Status
                    job_id = form.get('job_id', None)
                    if not job_id:
                        return json.dumps({'error': 'need a job'})
                    status = monitor(job_id)
                    return json.dumps({'status': status})

                elif action == 'run':
                    return render_template('identify/topic.html', field=field, topic=topic, topic_id=topic_id, rank_method=rank_method, window_size=window_size, top_n=top_n, page_num=page_num)

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
                            return render_template('identify/topic.html', from_external=True)
                        elif request_method == 'POST':
        #form data
                            form = request.form

        #follow actions: rank|previous_rank|check_status
                            action = form.get('action', None)
        #field
                            field = form.get('field', None)
        #topic or sub-field
                            topic = form.get('topic', None)
                            topic_id = form.get('topic_id', None)
        #total results count
                            top_n = int(form.get('top_n', 2000))
        #results count for every page
                            page_num = int(form.get('page_num', 20))
        #window size for idenfity i.e. adding time limit on input data
                            window_size = int(form.get('window_size', 1))
        #use PageRank if and only if window size equals 1
                            if window_size == 1 or window_size == 7:
                                rank_method = 'pagerank'
                            else:
                                rank_method = 'degree'

        #acquire topic id
                            if not topic_id:
                                if topic:
                                    topic_id = acquire_topic_id(topic)
                                else:
                                    if action == 'run':
                                        flash(u'请输入关键词！')
                                        return render_template('identify/topic.html', from_external=True)
                                    else:
                                        return json.dumps({'error': 'need a topic'})

        # current_time = time.time()
                            current_time = datetime2ts('2013-3-7')
                            if action == 'rank':
                                current_date = ts2datetime(current_time)
                                current_data = read_rank_results(top_n, 'topic', rank_method, current_date, window_size, topic_id=topic_id, compare=True)
                                if not current_data:
                                    if rank_method == 'pagerank':
                                        rank_func = getattr(areaModule, '%s_rank' % rank_method, None)
                                        if rank_func:
                                            current_data = rank_func(top_n, current_date, topic_id, window_size)
                                    elif rank_method == 'degree':
                                        rank_func = getattr(areaModule, '%s_rank' % rank_method, None)
                                        if rank_func:
                                            current_data = rank_func(top_n, current_date, topic_id, window_size)
                                return json.dumps({'status': 'current finished', 'data': current_data})

                            elif action == 'previous_rank':
                                previous_date = ts2datetime(current_time-window2time(window_size))
                                previous_data = read_rank_results(top_n, 'topic', rank_method, previous_date, window_size, topic_id=topic_id)
                                if not previous_data and window_size <= 7:
                                    if rank_method == 'pagerank':
                                        rank_func = getattr(areaModule, '%s_rank' % rank_method, None)
                                        if rank_func:
                                            previous_data = rank_func(top_n, previous_date, topic_id, window_size)
                                    elif rank_method == 'degree':
                                        rank_func = getattr(areaModule, '%s_rank' % rank_method, None)
                                        if rank_func:
                                            previous_data = rank_func(top_n, previous_date, topic_id, window_size)
                                return json.dumps({'status': 'previous finished', 'data': previous_data})

                            elif action == 'check_rank_status':
            #check Hadoop Job Status
                                job_id = form.get('job_id', None)
                                if not job_id:
                                    return json.dumps({'error': 'need a job'})
                                status = monitor(job_id)
                                return json.dumps({'status': status})

                            elif action == 'run':
                                return render_template('identify/topic.html', field=field, topic=topic, topic_id=topic_id, rank_method=rank_method, window_size=window_size, top_n=top_n, page_num=page_num)

                            else:
                                abort(404)
                        else:
                            abort(404)
                    else:
                        return redirect('/')
            return redirect('/')
    else:
        return redirect('/')


@mod.route("/area/network/", methods=["POST"])
def area_network():
    request_method = request.method
    if request_method == 'POST':
        # current_time = time.time()
        current_time = datetime2ts('2013-3-7')
        current_date = ts2datetime(current_time)
        gexf = None
        form = request.form
        topic_id = int(form.get('topic_id', None))
        window_size = int(form.get('window_size', 1))
        if not topic_id:
            gexf = ''
        else:
            gexf = areaModule.make_network_graph(current_date, topic_id, window_size)
        response = make_response(gexf)
        response.headers['Content-Type'] = 'text/xml'
        return response
    else:
        abort(404)
    
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

@mod.route("/add_kd/", methods=["POST"])
def add_kd():
    form = request.form
    uids_str = form.get('uids', None)
    if not uids_str:
        return json.dumps({'status': 'empty uids'})
    else:
        uids = map(int, uids_str.split(','))
        return json.dumps({'status': 'ok'})

@mod.route("/remove_kd/", methods=["POST"])
def remove_kd():
    form = request.form
    uids_str = form.get('uids', None)
    if not uids_str:
        return json.dumps({'status': 'empty uids'})
    else:
        uids = map(int, uids_str.split(','))
        return json.dumps({'status': 'ok'})

@mod.route("/add_trash/", methods=["POST"])
def add_trash():
    form = request.form
    uids_str = form.get('uids', None)
    if not uids_str:
        return json.dumps({'status': 'empty uids'})
    else:
        uids = map(int, uids_str.split(','))
        return json.dumps({'status': 'ok'})
