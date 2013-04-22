#-*-coding:utf-8-*-
"""突发特征词模型
"""

import json
import time
import codecs
import csv
import os
import math
import operator
from model import Words, PersonalBurstWords
from datetime import datetime
from datetime import date
from config import db

def unix2local(ts):
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts))

def timestr2ts(date):
    return time.mktime(time.strptime(date, '%a %b %d %H:%M:%S +0800 %Y'))

def date2ts(date):
    return time.mktime(time.strptime(date, '%Y-%m-%d'))

def partition(ts_arr, data, window_size=24*60*60):
    ts_series = []
    ts_start = ts_arr[0]
    ts_end = ts_arr[-1]
    each_step = window_size
    ts_current = ts_start
    data_cursor = -1
    groups_size = []
    groups_keywords_count = []
    while ts_current <= ts_end:
        s_ts = ts_current
        f_ts = ts_current + each_step
        ts_series.append([s_ts, f_ts])
        group = []
        for d in data[data_cursor+1:]:
            ts = d['ts']
            group.append(d)
            if ts >= f_ts:
                break
            data_cursor += 1
        if len(group):
            group_keywords_count = {}
            for d in group:
                for k in d['keywords']:
                    if k not in group_keywords_count:
                        group_keywords_count[k] = 0
                    group_keywords_count[k] += 1
            groups_keywords_count.append(group_keywords_count)
            groups_size.append(len(group))
        ts_current += each_step
    return ts_series, groups_keywords_count, groups_size

def burst(ts_series, groups_keywords_count, total_keywords_count, groups_size, total_size):
    word_burst_in_groups = []
    for period, group_keywords_count, group_size in zip(ts_series, groups_keywords_count, groups_size):
        word_burst_in_group = {}
        for keyword in group_keywords_count.keys():
            A = group_keywords_count[keyword]
            B = total_keywords_count[keyword] - A
            C = group_size - A
            D = total_size - total_keywords_count[keyword] - C
            try:
                word_burst_in_group[keyword] = (A + B + C + D) * ((A*D - B*C) ** 2) * 1.0 / ((A + B) * (C + D) * (A + C) * (B + D))
            except ZeroDivisionError:
                raise
                word_burst_in_group[keyword] = 0
        word_burst_in_groups.append(word_burst_in_group)
    keywords_burst = {}
    for keyword in total_keywords_count.keys():
        for group in word_burst_in_groups:
            if keyword in group:
                if keyword not in keywords_burst:
                    keywords_burst[keyword] = 0
                keywords_burst[keyword] += group[keyword]
    return keywords_burst

def hot(ts_series, groups_keywords_count, total_keywords_count, groups_size):
    word_hot_in_groups = []
    for period, group_keywords_count, group_size in zip(ts_series, groups_keywords_count, groups_size):
        word_hot_in_group = {}
        for keyword in group_keywords_count.keys():
            N = group_keywords_count[keyword]
            word_hot_in_group[keyword] = N * 1.0 / (group_size) 
        word_hot_in_groups.append(word_hot_in_group)
    keywords_hot = {}
    for keyword in total_keywords_count.keys():
        for group in word_hot_in_groups:
            if keyword in group:
                if keyword not in keywords_hot:
                    keywords_hot[keyword] = 0
                keywords_hot[keyword] += group[keyword]
    return keywords_hot

def search(begin, end, uidnumber=None):
    starttime = datetime.fromtimestamp(date2ts(begin))
    endtime = datetime.fromtimestamp(date2ts(end))
    ts_arr = []
    results = []
    total_keywords_count = {}
    if uidnumber:
        raw = Words.query.filter(Words.uid == uidnumber, Words.postDate > starttime, Words.postDate < endtime).all()
    else:
        raw = Words.query.filter(Words.postDate > starttime, Words.postDate < endtime).all()
    for word in raw:
        result = {}
        result['ts'] = time.mktime(word.postDate.timetuple())
        result['keywords'] = filter(lambda k: len(k) > 1, word.textWord.split(','))
        for k in result['keywords']:
            if k not in total_keywords_count:
                total_keywords_count[k] = 0
            total_keywords_count[k] += 1
        ts_arr.append(result['ts'])
        results.append(result)
        
    return ts_arr, results, total_keywords_count

def burst_model(time_start, time_end, uidnumber=None, window_size = 24*60*60):
    print 'start model from %s to %s at %s.' % (time_start, time_end, unix2local(time.time()))
    
    if uidnumber:
        ts_arr, results, total_keywords_count = search(time_start, time_end, uidnumber)
    else:
        ts_arr, results, total_keywords_count = search(time_start, time_end)
    total_size = len(results)
    print 'find %s statuses from %s to %s at %s.' % (total_size, time_start, time_end, unix2local(time.time()))

    ts_series, groups_keywords_count, groups_size = partition(ts_arr, results, window_size=window_size)
    print 'data partition ok at %s.' % unix2local(time.time())

    keywords_burst = burst(ts_series, groups_keywords_count, total_keywords_count, groups_size, total_size)
    print 'calculate keyword burst ok at %s.' % unix2local(time.time())

    keywords_hot = hot(ts_series, groups_keywords_count, total_keywords_count, groups_size)
    print 'calculate keywork hot ok at %s.' % unix2local(time.time())

    keywords_value = {}
    for keyword in total_keywords_count.keys():
        keywords_value[keyword] = keywords_hot[keyword] + math.log(keywords_burst[keyword] + 1, 2)
    print 'calculate keyword value ok at %s.' % unix2local(time.time())

    keywords_rank_list = sorted(keywords_value.iteritems(), key=operator.itemgetter(1), reverse=True)
    print 'keyword rank ok at %s.' % unix2local(time.time())

    
    for key, value in keywords_rank_list[:100]:
        item = PersonalBurstWords(uid=uidnumber, windowSize=window_size, startDate=date.fromtimestamp(date2ts(time_start)),
                                  endDate=date.fromtimestamp(date2ts(time_end)), word=key, burst=value, freq=total_keywords_count[key])
        db.session.add(item)
        db.session.commit()
        
    print 'write top 100 results into database.'
    

if __name__ == '__main__':
    #burst_model('2013-01-20', '2013-03-01')
    burst_model('2013-02-20', '2013-02-28')
