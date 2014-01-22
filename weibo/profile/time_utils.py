# -*- coding: utf-8 -*-

import time
import datetime
from datetime import date, datetime

def unix2hadoop_date(ts):
    return time.strftime('%Y_%m_%d', time.localtime(ts))

def ts2datetime(ts):
    return time.strftime('%Y-%m-%d', time.localtime(ts))

def datetime2ts(date):
    return time.mktime(time.strptime(date, '%Y-%m-%d'))

def window2time(window, size=24*60*60):
    return window*size

def ts2HMS(ts):
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts))

def datetimestr2ts(date):
    return time.mktime(time.strptime(date, '%Y%m%d'))

def ts2datetimestr(ts):
    return time.strftime('%Y%m%d', time.localtime(ts))

def ts2date(ts):
    return date.fromtimestamp(int(float(ts)))

def last_week_to_date(datestr, interval):
    # datestr: 20130907
    dates = []
    ts = datetimestr2ts(datestr)

    for i in range(0, interval):
        now_ts = ts - i * 24 * 3600
        date = ts2datetimestr(now_ts)
        dates.append(date)

    dates.reverse()

    return dates

def local2datetime(time_str):
    time_format = '%a %b %d %H:%M:%S +0800 %Y'
    return datetime.fromtimestamp(int(time.mktime(time.strptime(time_str, time_format))))
