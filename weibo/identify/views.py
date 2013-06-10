# -*- coding: utf-8 -*-

import os
import sys
import time
import subprocess
try:
    import simplejosn as json
except ImportError:
    import json

from flask import Blueprint, url_for, render_template, request, abort, flash, make_response

import whole as wholeModule

import area as areaModule

import burst as burstModule

import burst_monitor as burstRealtimeModule

from show_user_statuses import user_statuses

from utils import acquire_topic_id, acquire_topic_name, read_rank_results
from time_utils import ts2datetime, datetime2ts, window2time

from hadoop_utils import generate_job_id, monitor, prepare_data

mod = Blueprint('identify', __name__, url_prefix='/identify')

@mod.route('/')
def index():
    return render_template('identify/index.html')

@mod.route("/whole/", methods=["GET", "POST"])
def whole():
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

@mod.route("/burst/", methods=["GET", "POST"])
def burst():
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

@mod.route("/area/", methods=["GET", "POST"])
def area():
    request_method = request.method
    if request_method == 'GET':
        return render_template('identify/area.html', from_external=True)
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
        if window_size == 1:
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
                    return render_template('identify/area.html', from_external=True)
                else:
                    return json.dumps({'error': 'need a topic'})

        # current_time = time.time()
        current_time = datetime2ts('2013-3-7')
        if action == 'rank':
            current_date = ts2datetime(current_time)
            current_data = read_rank_results(top_n, 'area', rank_method, current_date, window_size, topic_id=topic_id, compare=True)
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
            previous_data = read_rank_results(top_n, 'area', rank_method, previous_date, window_size, topic_id=topic_id)
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
            return render_template('identify/area.html', field=field, topic=topic, topic_id=topic_id, rank_method=rank_method, window_size=window_size, top_n=top_n, page_num=page_num)

        else:
            abort(404)
    else:
        abort(404)

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
        current_time = datetime2ts('2013-3-7') + 12*60*60
        data = burstRealtimeModule.realtime_burst_user(top_n, current_time)
        return json.dumps(data)
    else:
        abort(404)

@mod.route("/statuses/<int:uid>/<int:page>/")
def show_user_statuses(uid, page):
    statuses = user_statuses(uid, page)
    return render_template('identify/user_statuses.html', statuses=statuses)
