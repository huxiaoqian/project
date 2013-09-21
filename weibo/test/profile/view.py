# -*- coding: utf-8 -*-

import os
import sys
import urllib2
import datetime
from datetime import date
import operator, random
import time
try:
    import simplejosn as json
except ImportError:
    import json

from flask import Flask, url_for, render_template, request, abort, flash

from utils import last_week, last_month, ts2date, getFieldUsersByScores, time2ts, datetime2ts

from xapian_weibo.xapian_backend import XapianSearch
xapian_search_user = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_user', schema_version=1)
xapian_search_weibo = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_weibo', schema_version=2)
xapian_search_weibo_test = XapianSearch(path='/opt/xapian_weibo/data/faked/', name='master_timeline_weibo', schema_version=2)

app = Flask(__name__)

@app.route('/profile/person/<uid>', methods=['GET', 'POST'])
def profile_person(uid):
    if uid:
        count, get_results = xapian_search_user.search(query={'_id': int(uid)}, fields=['profile_image_url', 'name', 'friends_count', \
                                          'statuses_count', 'followers_count', 'gender', 'verified', 'created_at', 'location'])
        if count > 0:
            for r in get_results():
                user = {'id': uid, 'profile_image_url': r['profile_image_url'], 'userName':  unicode(r['name'], 'utf-8'), 'friends_count': r['friends_count'], \
                        'statuses_count': r['statuses_count'], 'followers_count': r['followers_count'], 'gender': r['gender'], \
                        'verified': r['verified'], 'created_at': r['created_at'], 'location': unicode(r['location'], "utf-8")}
        else:
            return 'no such user'
    return json.dumps(user)

@app.route('/profile/person_count_false/<uid>', methods=['GET', 'POST'])
def personal_weibo_count_false(uid):
    total_days = 89
    today = datetime.datetime.today()
    now_ts = time.mktime(datetime.datetime(today.year, today.month, today.day, 2, 0).timetuple())
    now_ts = int(now_ts)
    during = 24 * 3600
    time_arr = []
    post_arr = []
    repost_arr = []
    fipost_arr = []

    m = request.args.get('m')

    if request.args.get('interval'):
        total_days =  int(request.args.get('interval')) - 1

    for i in range(total_days-1, -1, -1):
        end_ts = now_ts - i * during
        begin_ts = end_ts - during 

        repost_query_dict = {
            'timestamp': {
                '$gt': begin_ts,
                '$lt': end_ts
            },
            'user': int(uid),
            'retweeted_status': '1'
        }

        post_query_dict = {
            'timestamp': {
                '$gt': begin_ts,
                '$lt': end_ts
            },
            'user': int(uid)
        }

        if m == 'test':
            post_count = xapian_search_weibo_test.search(query=post_query_dict, count_only=True)
            repost_count = xapian_search_weibo_test.search(query=repost_query_dict, count_only=True)
            fipost_count = post_count - repost_count
        else:
            post_count = xapian_search_weibo.search(query=post_query_dict, count_only=True)
            repost_count = xapian_search_weibo.search(query=repost_query_dict, count_only=True)
            fipost_count = post_count - repost_count

        post_arr.append(post_count)
        fipost_arr.append(fipost_count)
        repost_arr.append(repost_count)
        time_arr.append(ts2date(end_ts).isoformat())
    print 'sum count: ', sum(post_arr)

    return json.dumps({'time': time_arr, 'count': post_arr, 'repost': repost_arr, 'fipost': fipost_arr})


@app.route('/profile/person_count/<uid>', methods=['GET', 'POST'])
def personal_weibo_count(uid):
    total_days = 89
    today = datetime.datetime.today()
    now_ts = time.mktime(datetime.datetime(today.year, today.month, today.day, 2, 0).timetuple())
    now_ts = int(now_ts)
    during = 24 * 3600
    time_arr = []
    post_arr = []
    repost_arr = []
    fipost_arr = []

    m = request.args.get('m')

    if request.args.get('interval'):
        total_days =  int(request.args.get('interval')) - 1

    for i in range(total_days-1, -1, -1):
        end_ts = now_ts - i * during
        begin_ts = end_ts - during 

        query_dict = {
            'timestamp': {
                '$gt': begin_ts,
                '$lt': end_ts
            },
            'user': int(uid),
        }

        if m == 'test':
            count, get_results = xapian_search_weibo_test.search(query=query_dict, fields=['retweeted_status'])
        else:
            count, get_results = xapian_search_weibo.search(query=query_dict, fields=['retweeted_status'])
        post_count = 0
        fipost_count = 0
        repost_count = 0
        for r in get_results():
            if r['retweeted_status']:
                repost_count += 1
            else:
                fipost_count += 1
            post_count += 1

        post_arr.append(post_count)
        fipost_arr.append(fipost_count)
        repost_arr.append(repost_count)
        time_arr.append(ts2date(end_ts).isoformat())
    print 'sum count: ', sum(post_arr)

    return json.dumps({'time': time_arr, 'count': post_arr, 'repost': repost_arr, 'fipost': fipost_arr})


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, port=9002)