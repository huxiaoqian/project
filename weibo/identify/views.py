# -*- coding: utf-8 -*-

import os
import sys
import time
import subprocess
try:
    import simplejosn as json
except ImportError:
    import json

from flask import Blueprint, url_for, render_template, request, abort, flash

import area as areaModule

import whole as wholeModule

from utils import acquire_id, acquire_value, read_rank_results
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
        action = form.get('action', 'check')

        top_n = form.get('top_n', 2000)
        page_num = form.get('page_num', 20)
        rank_method = form.get('rank_method', 'followers')
        window_size = form.get('window_size', 1)

        if action == 'rank':
            current_date = ts2datetime(time.time())
            data = read_rank_results(top_n, current_date, topic_id=None, r='whole', m=rank_method, w=window_size, compare=True)
            if not data:
                if rank_method == 'followers':
                    data = wholeModule.followers_rank(top_n, current_date, window_size)
                elif rank_method == 'active':
                    data = wholeModule.active_rank(top_n, current_date, window_size)
                elif rank_method == 'important':
                    data = wholeModule.important_rank(top_n, current_date, window_size)
            return json.dumps({'status': 'current finished', 'data': data})
        elif action == 'previous_rank':
            previous_date = ts2datetime(time.time()-window2time(window_size))
            previous_data = read_rank_results(top_n, previous_date, topic_id=None, r='whole', m=rank_method, w=window_size)
            if not previous_data:
                if rank_method == 'followers':
                    previous_data = wholeModule.followers_rank(top_n, previous_date, window_size)
                elif rank_method == 'active':
                    previous_data = wholeModule.active_rank(top_n, previous_date, window_size)
                elif rank_method == 'important':
                    previous_data = wholeModule.important_rank(top_n, previous_date, window_size)
            return json.dumps({'status': 'previous finished', 'data': previous_data})
        elif action == 'run':
            return render_template('identify/whole.html', top_n=top_n, page_num=page_num)
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
        action = form.get('action', 'check')

        top_n = form.get('top_n', 2000)
        page_num = form.get('page_num', 20)
        rank_method = form.get('rank_method', 'comments')
        window_size = form.get('window_size', 1)

        if action == 'rank':
            data = read_current_results(top_n, topic_id=None, r='burst', m=rank_method, w=window_size)
            return json.dumps({'status': 'current finished', 'data': data})
        elif action == 'previous_rank':
            previous_data = read_previous_results(top_n, topic_id=None, r='burst', m=rank_method, w=window_size)
            return json.dumps({'status': 'previous finished', 'data': previous_data})
        elif action == 'run':
            return render_template('identify/burst.html', top_n=top_n, page_num=page_num)
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

        #use Hadoop distributed computing platform or just in-memory computing
        hadoop = int(form.get('hadoop', 0))

        #acquire topic id
        if not topic_id:
            topic_id = acquire_id('Topic', topic)
        
        if action == 'simple_rank':
            #simple in-memory rank
            current_date = ts2datetime(time.time())
            data = read_rank_results(top_n, current_date, topic_id=topic_id, r='area', m=rank_method, w=window_size, compare=True)
            if data:
                return json.dumps({'status': 'current finished', 'data': data})
            else:
                if rank_method == 'pagerank':
                    data = areaModule.simple_pagerank(top_n, current_date, topic_id, window_size)
                else:
                    data = areaModule.simple_degreerank(top_n, current_date, topic_id, window_size)
                return json.dumps({'status': 'current finished', 'data': data})

        elif action == 'hadoop_rank':
            job_id = generate_job_id(topic_id)
            iter_count = 3
            #prepare for network data
            tmp_file = prepare_data(topic_id)
            input_tmp_path = tmp_file.name
            print 'prepare Hadoop Job %s ...' % job_id
            #launch Hadoop Job
            pagerank_cmd = 'python %s/weibo/identify/pagerank.py -j %s -i %s -c %s' % (os.getcwd(), job_id, input_tmp_path, iter_count)
            print 'Command %s ' % pagerank_cmd
            process = subprocess.Popen(pagerank_cmd,
                                       stdin=subprocess.PIPE,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE,
                                       shell=True)

            print 'Hadoop Job %s has been launched!' % job_id
            tmp_file.close()
            return json.dumps({'status': 'running'})

        elif action == 'previous_rank':
            previous_date = ts2datetime(time.time()-window2time(window_size))
            previous_data = read_rank_results(top_n, previous_date, topic_id=topic_id, r='area', m=rank_method, w=window_size)
            if previous_data:
                return json.dumps({'status': 'previous finished', 'data': previous_data})
            else:
                if rank_method == 'pagerank':
                    previous_data = areaModule.simple_pagerank(top_n, previous_date, topic_id, window_size)
                else:
                    previous_data = areaModule.simple_degreerank(top_n, previous_date, topic_id, window_size)
                return json.dumps({'status': 'previous finished', 'data': previous_data})

        elif action == 'check_status':
            #check Hadoop Job Status
            job_id = generate_job_id(topic_id)
            status = monitor(job_id)
            if status == 'finished':
                data = hadoop_results(topic_id, top_n,)
                if data == 'results_not_prepared':
                    return json.dumps({'status': 'results_not_prepared'})
                return json.dumps({'status': status, 'data': data})
            else:
                return json.dumps({'status': status})

        elif action == 'run':
            if not topic:
                flash(u'请输入关键词！')
                return render_template('identify/area.html', hadoop=hadoop, from_external=True)
            return render_template('identify/area.html', hadoop=hadoop, field=field, topic=topic, topic_id=topic_id, top_n=top_n, page_num=page_num)
        else:
            abort(404)
    else:
        abort(404)
