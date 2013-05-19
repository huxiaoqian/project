# -*- coding: utf-8 -*-

from flask import Blueprint, render_template
from xapian_weibo.xapian_backend_extra import Schema
from xapian_weibo.xapian_backend import XapianSearch
from xapian_weibo.utils import top_keywords
import simplejson as json
import datetime
import time

mod = Blueprint('moodlens', __name__, url_prefix='/moodlens')

emotions_kv = {'happy': 1, 'angry': 2, 'sad': 3}
s = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_sentiment', schema=Schema, schema_version=1)
total_days = 90


@mod.route('/')
def index():
    return render_template('moodlens/index.html', active='moodlens')


@mod.route('/data/<emotion>/')
def data(emotion):
    data = []

    today = datetime.datetime.today()
    now_ts = time.mktime(datetime.datetime(today.year, today.month, today.day, 2, 0).timetuple())
    now_ts = int(now_ts)
    during = 24 * 3600

    for i in xrange(-total_days + 1, 1):
        begin_ts = now_ts + during * (i - 1)
        end_ts = now_ts + during * i
        print i, begin_ts, end_ts
        query_dict = {
            'timestamp': {'$gt': begin_ts, '$lt': end_ts},
            'sentiment': emotions_kv[emotion],
        }
        count, _ = s.search(query=query_dict)

        data.append([end_ts * 1000, count])

    print data
    return json.dumps(data)


@mod.route('/flag_data/<emotion>/')
def flag_data(emotion):
    data = []

    today = datetime.datetime.today()
    now_ts = time.mktime(datetime.datetime(today.year, today.month, today.day, 2, 0).timetuple())
    now_ts = int(now_ts)
    during = 24 * 3600

    iter_count = 0
    for i in xrange(-total_days + 5, 1, 10):
        iter_count += 1
        begin_ts = now_ts + during * (i - 1)
        end_ts = now_ts + during * i
        query_dict = {
            'timestamp': {'$gt': end_ts - 3600, '$lt': end_ts},
            'sentiment': emotions_kv[emotion],
        }
        count, get_results = s.search(query=query_dict, fields=['terms'])
        print count
        keywords_with_count = top_keywords(get_results, top=10)
        text = ','.join([tp[0] for tp in keywords_with_count])
        data.append({
            'x': end_ts * 1000,
            'title': chr(ord('A') + iter_count),
            'text': text
        })

    return json.dumps(data)
