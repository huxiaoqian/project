# -*- coding: utf-8 -*-

import os
import sys
import time
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


mod = Blueprint('identify', __name__, url_prefix='/identify')

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

                top_n = int(form.get('top_n', 2000))
        #limit max results count to 2000
                if top_n > 2000:
                    top_n = 2000
                page_num = int(form.get('page_num', 20))
                rank_method = form.get('rank_method', 'followers')
                window_size = int(form.get('window_size', 1))

            # current_time = time.time()
                current_time = datetime2ts('2013-3-7')
                if action == 'rank':
                    current_date = ts2datetime(current_time)
                    data = read_rank_results(top_n, 'whole', rank_method, current_date, window_size, compare=True)
                    if not data:
                        rank_func = getattr(wholeModule, '%s_rank' % rank_method, None)
                        if rank_func:
                            data = rank_func(top_n, current_date, window_size)
                    return json.dumps({'status': 'current finished', 'data': data})
                elif action == 'previous_rank':
                    previous_date = ts2datetime(current_time-window2time(window_size))
                    previous_data = read_rank_results(top_n, 'whole', rank_method, previous_date, window_size)
                    if not previous_data and window_size <= 7:
                        rank_func = getattr(wholeModule, '%s_rank' % rank_method, None)
                        if rank_func:
                           previous_data = rank_func(top_n, previous_date, window_size)
                    return json.dumps({'status': 'previous finished', 'data': previous_data})
                elif action == 'run':
                    return render_template('identify/whole.html', rank_method=rank_method, window_size=window_size, top_n=top_n, page_num=page_num)
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

                            top_n = int(form.get('top_n', 2000))
        #limit max results count to 2000
                            if top_n > 2000:
                                top_n = 2000
                            page_num = int(form.get('page_num', 20))
                            rank_method = form.get('rank_method', 'followers')
                            window_size = int(form.get('window_size', 1))

            # current_time = time.time()
                            current_time = datetime2ts('2013-3-7')
                            if action == 'rank':
                                current_date = ts2datetime(current_time)
                                data = read_rank_results(top_n, 'whole', rank_method, current_date, window_size, compare=True)
                                if not data:
                                    rank_func = getattr(wholeModule, '%s_rank' % rank_method, None)
                                    if rank_func:
                                        data = rank_func(top_n, current_date, window_size)
                                return json.dumps({'status': 'current finished', 'data': data})
                            elif action == 'previous_rank':
                                previous_date = ts2datetime(current_time-window2time(window_size))
                                previous_data = read_rank_results(top_n, 'whole', rank_method, previous_date, window_size)
                                if not previous_data and window_size <= 7:
                                    rank_func = getattr(wholeModule, '%s_rank' % rank_method, None)
                                    if rank_func:
                                        previous_data = rank_func(top_n, previous_date, window_size)
                                return json.dumps({'status': 'previous finished', 'data': previous_data})
                            elif action == 'run':
                                return render_template('identify/whole.html', rank_method=rank_method, window_size=window_size, top_n=top_n, page_num=page_num)
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

                top_n = int(form.get('top_n', 2000))

                if top_n > 2000:
                    top_n = 2000
                page_num = int(form.get('page_num', 20))
                rank_method = form.get('rank_method', 'followers')
                window_size = int(form.get('window_size', 1))

                current_time = datetime2ts('2013-3-7')
                if action == 'rank':
                    current_date = ts2datetime(current_time)
                    data = read_rank_results(top_n, 'burst', rank_method, current_date, window_size, compare=True)
                    if not data:
                        rank_func = getattr(burstModule, '%s_rank' % rank_method, None)
                        if rank_func:
                            data = rank_func(top_n, current_date, window_size)
                    return json.dumps({'status': 'current finished', 'data': data})
                elif action == 'previous_rank':
                    previous_date = ts2datetime(current_time-window2time(window_size))
                    previous_data = read_rank_results(top_n, 'burst', rank_method, previous_date, window_size)
                    if not previous_data and window_size <= 7:
                        rank_func = getattr(burstModule, '%s_rank' % rank_method, None)
                        if rank_func:
                            previous_data = rank_func(top_n, previous_date, window_size)
                    return json.dumps({'status': 'previous finished', 'data': previous_data})
                elif action == 'run':
                    return render_template('identify/burst.html', rank_method=rank_method, window_size=window_size, top_n=top_n, page_num=page_num)
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

                            top_n = int(form.get('top_n', 2000))
        #limit max results count to 2000
                            if top_n > 2000:
                                top_n = 2000
                            page_num = int(form.get('page_num', 20))
                            rank_method = form.get('rank_method', 'followers')
                            window_size = int(form.get('window_size', 1))

        # current_time = time.time()
                            current_time = datetime2ts('2013-3-7')
                            if action == 'rank':
                                current_date = ts2datetime(current_time)
                                data = read_rank_results(top_n, 'burst', rank_method, current_date, window_size, compare=True)
                                if not data:
                                    rank_func = getattr(burstModule, '%s_rank' % rank_method, None)
                                    if rank_func:
                                        data = rank_func(top_n, current_date, window_size)
                                    return json.dumps({'status': 'current finished', 'data': data})
                            elif action == 'previous_rank':
                                previous_date = ts2datetime(current_time-window2time(window_size))
                                previous_data = read_rank_results(top_n, 'burst', rank_method, previous_date, window_size)
                                if not previous_data and window_size <= 7:
                                    rank_func = getattr(burstModule, '%s_rank' % rank_method, None)
                                    if rank_func:
                                        previous_data = rank_func(top_n, previous_date, window_size)
                                return json.dumps({'status': 'previous finished', 'data': previous_data})
                            elif action == 'run':
                                return render_template('identify/burst.html', rank_method=rank_method, window_size=window_size, top_n=top_n, page_num=page_num)
                            else:
                               abort(404)
                        else:
                            abort(404)
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
                top_n = int(form.get('top_n', 2000))
        #limit max results count to 2000
                if top_n > 2000:
                    top_n = 2000
                page_num = int(form.get('page_num', 20))
                rank_method = form.get('rank_method', 'followers')
                window_size = int(form.get('window_size', 1))
                field = form.get('field', 'finance')
                sort_by_field = rank_method + '_count'
                #page = int(request.form['page'])
                #if page == 1:
                #    startoffset = 0
                #else:
                ##    startoffset = (page - 1) * page_num
                startoffset = 0
                endoffset = startoffset + page_num - 1
                fieldEnName = field
                print 'here!',top_n,'here'
                count, field_users = xapian_search_domain.search(query={'domain':str(fields_id[str(fieldEnName)])}, sort_by=[sort_by_field], fields=['_id', 'name', 'statuses_count', 'friends_count', 'followers_count', 'profile_image_url', 'description'], max_offset=top_n)
                users = []
                count = 0
                for field_user in field_users():#[startoffset: endoffset]:
                    #if count < startoffset:
                    #    count += 1
                    #    continue
                    #if count > endoffset:
                    #    break
                    field_user['id'] = field_user['_id']
                    f_id = field_user['_id']
                    field_user['userName'] = field_user['name']
                    f_name = field_user['name']
                    field_user['statusesCount'] = field_user['statuses_count']
                    f_statusesCount = field_user['statuses_count']
                    field_user['friendsCount'] = field_user['friends_count']
                    f_friendsCount = field_user['friends_count']
                    field_user['followersCount'] = field_user['followers_count']
                    f_followersCount = field_user['followers_count']
                    
                    #users.append(field_user)
                    #users['status'] == 'current finished'
                    row = (f_id, f_name, f_followersCount, f_friendsCount, f_statusesCount)
                    users.append(row)
                    count += 1
                #return json.dumps(users)
                if action == 'rank':
                    current_data = users
                    return json.dumps({'status': 'current finished', 'data': current_data})
                elif action == 'run':
                    print 'here!',rank_method,window_size,field,top_n,page_num
                    return render_template('identify/area.html', rank_method=rank_method, window_size=window_size, top_n=top_n, page_num=page_num, field=field)
                elif action == 'previous_rank':
                    return json.dumps({'status': 'previous finished', 'data': current_data})
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

                            top_n = int(form.get('top_n', 2000))
        #limit max results count to 2000
                        if top_n > 2000:
                            top_n = 2000
                        page_num = int(form.get('page_num', 20))
                        rank_method = form.get('rank_method', 'followers')
                        window_size = int(form.get('window_size', 1))
                        field = form.get('field', None)
                        sort_by_field = rank_method + '_count'

                        page = int(request.form['page'])
                        if page == 1:
                            startoffset = 0
                        else:
                            startoffset = (page - 1) * page_num
                        endoffset = startoffset + page_num - 1
                        fieldEnName = field
                        count, field_users = xapian_search_domain.search(query={'domain':str(fields_id[str(fieldEnName)])}, sort_by=[sort_by_field], fields=['_id', 'name', 'statuses_count', 'friends_count', 'followers_count', 'profile_image_url', 'description'], max_offset=10000)
                        users = []
                        count = 0
                        for field_user in field_users():#[startoffset: endoffset]:
                            if count < startoffset:
                                count += 1
                                continue
                            if count > endoffset:
                                break
                            field_user['id'] = field_user['_id']
                            f_id = field_user['_id']
                            field_user['profileImageUrl'] = field_user['profile_image_url']
                            f_image = field_user['profile_image_url']
                            field_user['userName'] = field_user['name']
                            f_name = field_user['name']
                            field_user['statusesCount'] = field_user['statuses_count']
                            f_statusesCount = field_user['statuses_count']
                            field_user['friendsCount'] = field_user['friends_count']
                            f_friendsCount = field_user['friends_count']
                            field_user['followersCount'] = field_user['followers_count']
                            f_followersCount = field_user['followers_count']
                            field_user['description'] = field_user['description']
                            f_description = field_user['description']
                            #users.append(field_user)
                            row = (f_id, f_image, f_name, f_statusesCount, f_friendsCount, f_followersCount, f_description)
                            users.append(row)
                            count += 1
                        return json.dumps(users)
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
        top_n = int(form.get('top_n', 10))
        # current_time = time.time()
        current_time = datetime2ts('2013-3-7') + 12*60*60
        data = burstRealtimeModule.realtime_burst_user(top_n, current_time)
        return json.dumps(data)
    else:
        abort(404)

@mod.route("/statuses/<int:uid>/<int:page>/")
def show_user_statuses(uid, page):
    if 'logged_in' in session and session['logged_in']:
        statuses = user_statuses(uid, page)
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
