# -*- coding: utf-8 -*-

from flask import Blueprint, render_template
from xapian_weibo.xapian_backend import XapianSearch
from riak.client import RiakClient
import simplejson as json
import datetime
import time

mod = Blueprint('moodlens', __name__, url_prefix='/moodlens')
s = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline')
client = RiakClient(host='219.224.135.60', pb_port=10017, protocol='pbc')
weibo_multi_sentiment = client.bucket('huyue_weibo_multi_sentiment')
emotions_kv = {'happy': 1, 'angry': 2, 'disgusting': 3}


@mod.route('/')
def index():
    return render_template('moodlens/index.html', active='moodlens')


@mod.route('/data/<emotion>/')
def data(emotion):
    daily_count_kv = {}
    total_days = 39

    today = datetime.datetime.today()
    now_ts = time.mktime(datetime.datetime(today.year, today.month, today.day, 2, 0).timetuple())
    now_ts = int(now_ts)
    during = 24 * 3600
    for i in xrange(-total_days + 1, 1):
        daily_count_kv[str(now_ts - during * i)] = 0

    begin_ts = now_ts - total_days * during
    query_dict = {
        'timestamp': {'$gt': begin_ts, '$lt': now_ts},
    }
    _, get_results = s.search(query=query_dict, fields=['id', 'timestamp'])
    for r in get_results():
        sentiment_data = weibo_multi_sentiment.get(str(r['id']))
        sentiment = sentiment_data.data
        if sentiment == emotions_kv[emotion]:
            for i in xrange(-total_days + 1, 1):
                gt = now_ts - during * (i + 1)
                lt = now_ts - during * i
                if gt < r['timestamp'] <= lt:
                    daily_count_kv[str(lt)] += 1
    data = [[int(x[0]) * 1000, x[1]] for x in list(daily_count_kv.items())]

    return json.dumps(data)
