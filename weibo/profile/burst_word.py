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
from datetime import datetime
from datetime import date
import leveldb
from utils import getFieldUsersByScores

from xapian_weibo.xapian_backend import XapianSearch
s_weibo = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_weibo')

from xapian_weibo.utils import load_scws, cut
s_cut = load_scws()

LEVELDBPATH = '/home/mirage/leveldb'
user_burst_topics_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_user_burst_topics'),
                                           block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
field_burst_topics_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_field_burst_topics'),
                                            block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

###获取停用词表
def getStopWds(filename):
    swds = set()
    f = open(filename, 'r')
    count = 0
    for line in f.readlines():
        word = line.split()[0]
        swds.add(word)
        count += 1
    print 'stop words : ', count
    return swds

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
    try:
        swds = getStopWds('./data/stpwds_linhao_20130514.txt')
    except:
        print os.getcwd()
        swds = getStopWds('/home/mirage/linhao/project_bishe/weibo/profile/data/stpwds_linhao_20130514.txt')
    begin_ts = date2ts(begin)
    end_ts = date2ts(end)
    query_dict = {
        'timestamp': {'$gt': begin_ts, '$lt': end_ts},
        'user': int(uidnumber)
    }
    count, get_results = s_weibo.search(query=query_dict, fields=['timestamp', 'text'], sort_by=['timestamp'])
    ts_arr = []
    results = []
    total_keywords_count = {}
    if count > 0:
        for r in get_results():
            ts_result = {}
            terms = cut(s_cut, r['text'], f=['n'])
            terms = [unicode(term, 'utf-8') for term in terms]
            terms_more_than_1_word = filter(lambda k: len(k) > 1, terms)
            keywords = filter(lambda k: k not in swds, terms_more_than_1_word)
            if len(keywords):
                for k in keywords:
                    if k not in total_keywords_count:
                        total_keywords_count[k] = 0
                    total_keywords_count[k] += 1
                ts_result['keywords'] = keywords
                ts_result['ts'] = int(r['timestamp'])
                results.append(ts_result)
                ts_arr.append(ts_result['ts'])
    else:
        return None
    return ts_arr, results, total_keywords_count

def search_field(begin, end, field):
    uids = getFieldUsersByScores(field, 0, 99)
    print len(uids)
    try:
        swds = getStopWds('./data/stpwds_linhao_20130514.txt')
    except:
        print os.getcwd()
        swds = getStopWds('/home/mirage/linhao/project_bishe/weibo/profile/data/stpwds_linhao_20130514.txt')
    begin_ts = date2ts(begin)
    end_ts = date2ts(end)
    ts_arr = []
    results = []
    total_keywords_count = {}
    for uidnumber in uids:
        query_dict = {
            'timestamp': {'$gt': begin_ts, '$lt': end_ts},
            'user': int(uidnumber)
        }
        count, get_results = s_weibo.search(query=query_dict, fields=['timestamp', 'text'], sort_by=['timestamp'])
        if count > 0:
            for r in get_results():
                ts_result = {}
                terms = cut(s_cut, r['text'], f=['n'])
                terms = [unicode(term, 'utf-8') for term in terms]
                terms_more_than_1_word = filter(lambda k: len(k) > 1, terms)
                keywords = filter(lambda k: k not in swds, terms_more_than_1_word)
                if len(keywords):
                    for k in keywords:
                        if k not in total_keywords_count:
                            total_keywords_count[k] = 0
                        total_keywords_count[k] += 1
                    ts_result['keywords'] = keywords
                    ts_result['ts'] = int(r['timestamp'])
                    results.append(ts_result)
    results_list = sorted(results, key=lambda d:d['ts'])
    for result in results_list:
        ts_arr.append(result['ts'])
    return ts_arr, results_list, total_keywords_count

def burst_model(time_start, time_end, uidnumber=None, window_size = 24*60*60, topn=1000):
    print 'start model from %s to %s at %s.' % (time_start, time_end, unix2local(time.time()))
    
    if uidnumber:
        search_result = search(time_start, time_end, uidnumber)
        if search_result:
            ts_arr, results, total_keywords_count = search_result
        else:
            return None
    else:
        return None
    total_size = len(results)
    print 'find %s statuses from %s to %s at %s.' % (total_size, time_start, time_end, unix2local(time.time()))

    ts_series, groups_keywords_count, groups_size = partition(ts_arr, results, window_size=window_size)
    print 'data partition ok at %s.' % unix2local(time.time())

    keywords_burst = burst(ts_series, groups_keywords_count, total_keywords_count, groups_size, total_size)
    print 'calculate keyword burst ok at %s.' % unix2local(time.time())
    print len(keywords_burst.keys())

    keywords_hot = hot(ts_series, groups_keywords_count, total_keywords_count, groups_size)
    print 'calculate keywork hot ok at %s.' % unix2local(time.time())
    print len(keywords_hot.keys())

    keywords_value = {}
    for keyword in total_keywords_count.keys():
        keywords_value[keyword] = keywords_hot[keyword] + math.log(keywords_burst[keyword] + 1, 2)
    print 'calculate keyword value ok at %s.' % unix2local(time.time())

    keywords_rank_list = sorted(keywords_value.iteritems(), key=operator.itemgetter(1), reverse=True)
    print 'keyword rank ok at %s.' % unix2local(time.time())

    word_dict = {}
    if len(keywords_rank_list) < topn:
        topn = len(keywords_rank_list)
    for word, bst in keywords_rank_list[:(topn-1)]:
        word_dict[word] = (bst, total_keywords_count[word])

    save_burst_words2db(uidnumber, time_start, time_end, window_size, word_dict)
    return word_dict

def burst_model_field(time_start, time_end, field=None, window_size = 24*60*60, topn=1000):
    print 'start model from %s to %s at %s.' % (time_start, time_end, unix2local(time.time()))
    
    if field:
        search_result = search_field(time_start, time_end, field)
        if search_result:
            ts_arr, results, total_keywords_count = search_result
        else:
            return None
    else:
        return None
    total_size = len(results)
    print 'find %s statuses from %s to %s at %s.' % (total_size, time_start, time_end, unix2local(time.time()))

    ts_series, groups_keywords_count, groups_size = partition(ts_arr, results, window_size=window_size)
    print 'data partition ok at %s.' % unix2local(time.time())

    keywords_burst = burst(ts_series, groups_keywords_count, total_keywords_count, groups_size, total_size)
    print 'calculate keyword burst ok at %s.' % unix2local(time.time())
    print len(keywords_burst.keys())

    keywords_hot = hot(ts_series, groups_keywords_count, total_keywords_count, groups_size)
    print 'calculate keywork hot ok at %s.' % unix2local(time.time())
    print len(keywords_hot.keys())

    keywords_value = {}
    for keyword in total_keywords_count.keys():
        keywords_value[keyword] = keywords_hot[keyword] + math.log(keywords_burst[keyword] + 1, 2)
    print 'calculate keyword value ok at %s.' % unix2local(time.time())

    keywords_rank_list = sorted(keywords_value.iteritems(), key=operator.itemgetter(1), reverse=True)
    print 'keyword rank ok at %s.' % unix2local(time.time())

    word_dict = {}
    if len(keywords_rank_list) < topn:
        topn = len(keywords_rank_list)
    for word, bst in keywords_rank_list[:(topn-1)]:
        word_dict[word] = (bst, total_keywords_count[word])

    save_field_burst_words2db(field, time_start, time_end, window_size, word_dict)
    return word_dict

def read_burst_wordsFromDb(uid, time_start, time_end, window_size=24*60*60):
    key = str(uid) + '_' + str(time_start) + '_' + str(time_end) + '_' + str(window_size)
    try:
        value = user_burst_topics_bucket.Get(key)
        topics = json.loads(value)
        return topics
    except KeyError:
        return None

def read_field_burst_wordsFromDb(field, time_start, time_end, window_size=24*60*60):
    key = str(field) + '_' + str(time_start) + '_' + str(time_end) + '_' + str(window_size)
    try:
        value = field_burst_topics_bucket.Get(key)
        topics = json.loads(value)
        return topics
    except KeyError:
        return None
    
def sort_busrt_words(topics, sort='freq', limit=200):
    results = []
    if sort == 'freq':
        sortedtopics = sorted(topics.iteritems(), key=lambda (k,v): v[1], reverse=True)
        for word, weight in sortedtopics[:limit]:
            results.append({'text': word, 'size': weight[1]})
        return results
    else:
        sortedtopics = sorted(topics.iteritems(), key=lambda (k,v): v[0], reverse=True)
        for word, weight in sortedtopics[:limit]:
            results.append({'text': word, 'size': weight[0]})
        return results

def save_burst_words2db(uid, time_start, time_end, window_size, words):
    key = str(uid) + '_' + str(time_start) + '_' + str(time_end) + '_' + str(window_size)
    value = json.dumps(words)
    user_burst_topics_bucket.Put(key, value)
    print 'write top results into database.'

def save_field_burst_words2db(field, time_start, time_end, window_size, words):
    key = str(field) + '_' + str(time_start) + '_' + str(time_end) + '_' + str(window_size)
    value = json.dumps(words)
    field_burst_topics_bucket.Put(key, value)
    print 'write top results into database.'

if __name__ == '__main__':
    burst_model('2013-03-10', '2013-06-10', uidnumber=1813080181)
