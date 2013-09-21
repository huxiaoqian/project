# -*- coding: utf-8 -*-

import time
from datetime import datetime, timedelta, date
from xapian_weibo.xapian_backend import XapianSearch
from flask import request
import json
import redis
import leveldb
import os
import operator

redis_host = 'localhost'
redis_port = 6379
redis_conn = redis.Redis(redis_host, redis_port)

LEVELDBPATH = '/home/mirage/leveldb'

xapian_search_weibo = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_weibo', schema_version=2)

def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print '%r %2.2f sec' % (method.__name__, te-ts)
        return result
    return timed


def getFieldUsersByScores(fieldName, start_offset, end_offset, update_date='20130430'):
    sorted_key = 'followers_count'
    sortedset_key = 'linhao_dailyfielduser_%s_%s_%s' % (update_date, fieldName, sorted_key)
    result = redis_conn.zrevrange(sortedset_key, start_offset, end_offset, withscores=False)
    return result

            
def local2datetime(time_str):
    time_format = '%a %b %d %H:%M:%S +0800 %Y'
    return datetime.fromtimestamp(int(time.mktime(time.strptime(time_str, time_format))))


def ts2datetime(ts):
     return datetime.fromtimestamp(int(float(ts)))


def datetime2ts(date):
    return time.mktime(time.strptime(date, '%Y-%m-%d'))


def ts2date(ts):
    return date.fromtimestamp(int(float(ts)))


def time2ts(date):
    return time.mktime(time.strptime(date, '%Y-%m-%d'))


def date2ts(datestr):
    return int(time.mktime(time.strptime(datestr, '%Y-%m-%d  %H:%M:%S')))


def last_week(weeks=1, interval=1):
    '''计算当前日期的上一周起止日期（从上周一到本周一）
        weeks为从本周往前回溯的周数
        interval为间隔的周数
    '''
    now_weekday = datetime.now().weekday()
    now_date = date.today()
    this_monday = now_date - timedelta(days=now_weekday) - timedelta(days=7*(weeks-1))
    last_monday = this_monday - timedelta(days=7*interval)
    return last_monday.isoformat(), this_monday.isoformat()

def last_month(mon_num=1):
    '''计算当前日期回溯若干月
    '''
    now_date = date.today()
    last_date = now_date - timedelta(days=30 * mon_num)
    return last_date.isoformat(), now_date.isoformat()

def last_day(day_num=1):
    now_date = date.today()
    last_date = now_date - timedelta(days=day_num)
    return last_date.isoformat(), now_date.isoformat()
    

def main():
    #last_week()
    getFieldUsersByScores('finance', 0, 19)
    pass
    
if __name__ == '__main__':
    print last_day(1)
            
