# -*- coding: utf-8 -*-

import os
import sys
import subprocess
try:
    import simplejosn as json
except ImportError:
    import json

from flask import Blueprint, url_for, render_template, request, abort, flash

from weibo.extensions import db
from utils import acquire_id, acquire_value, prepare_data, \
    read_current_results, read_previous_results
from hadoop_utils import generate_job_id, monitor, read_hadoop_results

mod = Blueprint('identify', __name__, url_prefix='/identify')

@mod.route('/')
def index():
    return render_template('identify/index.html')

@mod.route("/whole/", methods=["GET", "POST"])
def whole():
    return render_template('identify/whole.html')

@mod.route("/burst/", methods=["GET", "POST"])
def burst():
    return render_template('identify/burst.html')

@mod.route("/area/", methods=["GET", "POST"])
def area():
    request_method = request.method
    if request_method == 'GET':
        return render_template('identify/area.html', from_external=True)
    elif request_method == 'POST':
        form = request.form
        action = form.get('action', 'check')
        field = form.get('field', None)
        keywords = form.get('keywords', None)
        top_n = form.get('top_n', 2000)
        page_num = form.get('page_num', 20)

        demo = form.get('demo', 0)

        field_id = acquire_id('Field', field)
        topic_id = acquire_id('Topic', keywords)
        
        #just for demo without Hadoop
        if demo:
            if action == 'rank':
                data = read_current_results(topic_id, top_n, demo=True)
                return json.dumps({'status': 'current finished', 'data': data})
            elif action == 'previous_rank':
                previous_data = read_previous_results(topic_id, top_n)
                return json.dumps({'status': 'previous finished', 'data': previous_data})
            elif action == 'run':
                if not keywords:
                    flash(u'请输入关键词！')
                    return render_template('identify/area.html', demo=True, from_external=True)
                
                return render_template('identify/area.html', field=field, keywords=keywords, top_n=top_n, page_num=page_num, demo=True)
            else:
                abort(404)

        #Hadoop
        job_id = generate_job_id(field_id, topic_id)
        iter_count = 3
        if action == 'check':
            #check Hadoop Job Status
            status = monitor(job_id)
            if status == 'finished':
                data = read_hadoop_results(job_id, top_n)
                if data == 'results_not_prepared':
                    return json.dumps({'status': 'results_not_prepared'})
                return json.dumps({'status': status, 'data': data})
            else:
                return json.dumps({'status': status})
        elif action == 'run':
            #launch Hadoop Job
            if not keywords:
                flash(u'请输入关键词！')
                return render_template('identify/area.html', from_external=True)
            #prepare for network data
            field = acquire_value('Field', field_id)
            topic = acquire_value('Topic', topic_id)
            tmp_file = prepare_data(field, topic)
            input_tmp_path = tmp_file.name

            print 'prepare Hadoop Job %s ...' % job_id
            pagerank_cmd = 'python %s/weibo/identify/pagerank.py -j %s -i %s -c %s' % (os.getcwd(), job_id, input_tmp_path, iter_count)
            print 'Command %s ' % pagerank_cmd
            process = subprocess.Popen(pagerank_cmd, 
                                       stdin=subprocess.PIPE, 
                                       stdout=subprocess.PIPE, 
                                       stderr=subprocess.PIPE, 
                                       shell=True)

            print 'Hadoop Job %s has been launched!' % job_id
            tmp_file.close()
            return render_template('identify/area.html', field=field, keywords=keywords, top_n=top_n, page_num=page_num)
        else:
            abort(404)
    else:
        abort(404)
