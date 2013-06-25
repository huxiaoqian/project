# -*- coding: utf-8 -*-

import time
from datetime import datetime, timedelta, date
from config import db
from config import app as current_app
from flask import request
from model import *
from sqlalchemy import func
import json
import redis
import leveldb
import os
import operator

redis_host = 'localhost'
redis_port = 6379
redis_conn = redis.Redis(redis_host, redis_port)

LEVELDBPATH = '/home/mirage/leveldb'

def is_in_black_list(uid):
    return False

def get_leveldb(method, ts):
    date = datetime.fromtimestamp(ts)
    db_name = 'hanyang_daily_user_%s_' % method
    db_name += str(date.year)
    if date.month < 10:
        db_name += '0%s' % str(date.month)
    else:
        db_name += str(date.month)
    if date.day < 10:
        db_name += '0%s' % str(date.day)
    else:
        db_name += str(date.day)
    return db_name

def active_rank(top_n, date, window_size):
    date_time = datetime2ts(date)
    uid_active = {}
    if window_size == 1:
        db_name = get_leveldb('active', date_time)
        daily_user_active_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, db_name),
                                              block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
        
        for uid, active in daily_user_active_bucket.RangeIter():
            uid = int(uid)
            active = float(active)
            uid_active[uid] = active
    else:
        for i in range(window_size):
            db_name = get_leveldb('active', date_time - i*24*60*60)
            daily_user_active_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, db_name),
                                              block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
            for uid, active in daily_user_active_bucket.RangeIter():
                uid = int(uid)
                active = float(active)
                if uid not in uid_active:
                    uid_active[uid] = 0
                uid_active[uid] += active

    sorted_uid_active = sorted(uid_active.iteritems(), key=operator.itemgetter(1), reverse=True)
                
    sorted_uids = []
    count = 0
    for uid, value in sorted_uid_active:
        if is_in_black_list(uid):
            continue
        if count >= top_n:
            break
        sorted_uids.append(uid)
        count += 1

    data = sorted_uids
    return data

def getFieldUsersByScores(fieldName, start_offset, end_offset, update_date='20130430'):
    sorted_key = 'followers_count'
    sortedset_key = 'linhao_dailyfielduser_%s_%s_%s' % (update_date, fieldName, sorted_key)
    result = redis_conn.zrevrange(sortedset_key, start_offset, end_offset, withscores=False)
    return result

def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print '%r %2.2f sec' % (method.__name__, te-ts)
        return result
    return timed

@timeit
def store2riak(get_results):
    count = 0
    for r in get_results():
        # 微博是否为转发微博
        weibo_is_retweet_status_bucket = client.bucket('lijun_weibo_is_retweet_status')
        is_retweet_status_bucket = 1 if weibo['retweeted_status'] else 0
        new_node = weibo_is_retweet_status_bucket.new(weibo['id'], data=is_retweet_status_bucket)
        new_node.store()
        count += 1

    print 'total store count:', count

@timeit
def test_riak_write(test_bucket, n):
    for i in range(n):
        test_bucket.new(str(i), data=i).store()

@timeit
def test_riak_read(test_bucket, n):
    for i in range(n):
        r = test_bucket.get(str(i))
        # stable version
        # data = r.get_data()
        # master_version
        data = r.data

        if data != i:
            raise
            
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

def hot_uid_by_word(starttime, endtime, count=50):
    '''筛选出词语表中有超过50条记录的博主
    '''
    startdate = ts2datetime(time2ts(starttime))
    enddate =  ts2datetime(time2ts(endtime))
    uids = set()
    uids_count = db.session.query(Words.uid, func.count(Words.id)).\
                               filter(Words.postDate>startdate, Words.postDate<enddate).\
                               group_by(Words.uid).\
                               having(func.count(Words.id) > count).all()
    for uid, count in uids_count:
        uids.add(uid)
    return uids

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
            
