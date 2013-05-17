# -*- coding: utf-8 -*-

from flask import Blueprint, render_template
from xapian_weibo.xapian_backend_extra import Schema
from xapian_weibo.xapian_backend import XapianSearch
import simplejson as json
import datetime
import time

mod = Blueprint('moodlens', __name__, url_prefix='/moodlens')

emotions_kv = {'happy': 1, 'angry': 2, 'sad': 3}
s = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_sentiment', schema=Schema, schema_version=1)


@mod.route('/')
def index():
    return render_template('moodlens/index.html', active='moodlens')


@mod.route('/data/<emotion>/')
def data(emotion):
    data = []
    total_days = 10

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
    total_days = 10

    today = datetime.datetime.today()
    now_ts = time.mktime(datetime.datetime(today.year, today.month, today.day, 2, 0).timetuple())
    now_ts = int(now_ts)
    during = 24 * 3600

    for i in xrange(-total_days + 1, 1, 4):
        begin_ts = now_ts + during * (i - 1)
        end_ts = now_ts + during * i
        data.append({
            'x': end_ts * 1000,
            'title': chr(ord('Z') + i),
            'text': 'hehe'
        })

    print data
    return json.dumps(data)
