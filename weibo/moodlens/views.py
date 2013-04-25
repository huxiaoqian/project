# -*- coding: utf-8 -*-

from flask import Blueprint, render_template
import simplejson as json
import datetime
import random
import time


mod = Blueprint('moodlens', __name__, url_prefix='/moodlens')


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
    for i in xrange(-89, 1):
        data.append([(now_ts + during * i) * 1000, random.randint(0, 10000)])

    return json.dumps(data)
