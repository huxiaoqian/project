# -*- coding: utf-8 -*-

from flask import Blueprint, render_template
import leveldb
import simplejson as json
import datetime
import time
import os

mod = Blueprint('moodlens', __name__, url_prefix='/moodlens')
LEVELDBPATH = '/home/mirage/leveldb'
buckets = {}
def get_bucket(bucket):
    if bucket in buckets:
        return buckets[bucket]
    buckets[bucket] = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'lijun_' + bucket),
                                                   block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    return buckets[bucket]

emotions_kv = {'happy': 1, 'angry': 2, 'sad': 3}


@mod.route('/')
def index():
    return render_template('moodlens/index.html', active='moodlens')


@mod.route('/data/<emotion>/')
def data(emotion):
    data = []
    total_days = 89

    today = datetime.datetime.today()
    now_ts = time.mktime(datetime.datetime(today.year, today.month, today.day, 2, 0).timetuple())
    now_ts = int(now_ts)
    during = 24 * 3600

    bucket = get_bucket('weibo_daily_sentiment_count')
    for i in xrange(-total_days + 1, 1):
        lt = now_ts + during * i
        try:
            daily_emotion_count = bucket.Get(str(lt) + '_' + str(emotions_kv[emotion]))
            daily_emotion_count = int(daily_emotion_count)
        except KeyError:
            daily_emotion_count = 0
        data.append([lt * 1000, daily_emotion_count])

    return json.dumps(data)
