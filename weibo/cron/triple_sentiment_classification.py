# -*- coding: cp936 -*-

#  gathering snmp data
from __future__ import division
##import subprocess
##import popen2
import os
import datetime
import random
import time
import nltk
import re
from gensim import corpora, models, similarities
import math
import string
from nltk import probability
from nltk.probability import FreqDist
import cPickle as pickle
import leveldb
from xapian_weibo.xapian_backend import XapianSearch
from xapian_weibo.utils import load_scws
from xapian_weibo.utils import cut


xs = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_weibo')
cut_str = load_scws()

##情绪类标
HAPPY = 1
ANGRY = 2
SAD = 3


def emoticon(zan_set, angry_set, sad_set, text):
    """ text是微博文本，不是关键词"""

    emotion_pattern = r'\[(\S+?)\]'
    remotions = re.findall(emotion_pattern, text)
    zan = 0
    angry = 0
    sad = 0

    if remotions:
        for e in remotions:
            if e in zan_set:
                zan = 1
            elif e in angry_set:
                angry = 1
            elif e in sad_set:
                sad = 1
    state = 0
    if zan == 1 and angry == 0 and sad == 0:
        state = HAPPY
    elif zan == 0 and angry == 1 and sad == 0:
        state = ANGRY
    elif zan == 0 and angry == 0 and sad == 1:
        state = SAD

    return state


print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
##从txt文件中获取选定积极、消极表情符号的集合
zan = set()
angry = set()
sad = set()

with open('4groups.csv') as f:
    for l in f:
        pair = l.rstrip().split('\t')
        if pair[1] == '1':
            zan.add(pair[0].decode('utf-8'))

        if pair[1] == '2':
            angry.add(pair[0].decode('utf-8'))

        if pair[1] == '3':
            sad.add(pair[0].decode('utf-8'))

        if pair[1] == '4':
            zan.add(pair[0].decode('utf-8'))


##读取字典
f = file('/home/mirage/senitmet/triple_sentiment.pkl', 'r')
dictionary = pickle.load(f)
f.close()


##读取各个词的权重信息
p_senti = {}
with open('/home/mirage/senitmet/triple_sentiment_words_weight.txt') as f:
    for l in f:
        try:
            lis = l.rstrip().split()
            p_senti[int(lis[0])] = [float(lis[1]), float(lis[2]), float(lis[3])]
        except:
            print l


##利用索引取3个月数据
total_days = 89
today = datetime.datetime.today()
now_ts = time.mktime(datetime.datetime(today.year, today.month, today.day, 2, 0).timetuple())
begin_ts = now_ts - total_days * 24 * 3600


query_dict = {
    'timestamp': {
        '$gt': begin_ts,
        '$lt': now_ts,
    }
}
count, get_results = xs.search(query=query_dict, fields=['_id', 'text', 'retweeted_status'])
print count


##对3个月的每条微博进行分类
LEVELDBPATH = '/home/mirage/leveldb'
weibo_is_retweet_status_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'lijun_weibo_is_retweet_status'),
                                                 block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
weibo_emoticoned_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'lijun_weibo_emoticoned'),
                                          block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
weibo_empty_retweet_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'lijun_weibo_empty_retweet'),
                                             block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
weibo_multi_sentiment_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'huyue_weibo_multi_sentiment'),
                                               block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

triple = [0, 0, 0]
print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
iter_count = 0
ts = te = time.time()
for r in get_results():
    iter_count += 1
    if iter_count % 10000 == 0:
        te = time.time()
        print iter_count, '%s sec' % (te - ts)
        ts = te
    sentiment = 0
    id_str = str(r['_id'])

    text = ''
    if_empty_retweet = weibo_empty_retweet_bucket.Get(id_str)
    if if_empty_retweet:
        if_empty_retweet = int(if_empty_retweet)
    if if_empty_retweet == 1:
        mid_id_str = str(r['retweeted_status'])
    else:
        mid_id_str = id_str

    if_emoticoned = weibo_emoticoned_bucket.Get(mid_id_str)
    if if_emoticoned:
        if_emoticoned = int(if_emoticoned)
    if if_emoticoned == 1:
        emoticon_sentiment = emoticon(zan, angry, sad, r['text'])
        if emoticon_sentiment == 1 or emoticon_sentiment == 2:
            sentiment = emoticon_sentiment
        else:
            text = r['text']
    else:
        text = r['text']

    if text != '':
        entry = cut(cut_str, text)
        bow = dictionary.doc2bow(entry)
        s = [1, 1, 1]
        for pair in bow:
            for rp in range(pair[1]):
                s[0] = s[0] * p_senti[pair[0]][0]
                s[1] = s[1] * p_senti[pair[0]][1]
                s[2] = s[2] * p_senti[pair[0]][2]
        if s[0] > s[1] and s[0] > s[2]:
            sentiment = HAPPY
        elif s[1] > s[0] and s[1] > s[2]:
            sentiment = ANGRY
        elif s[2] > s[1] and s[2] > s[0]:
            sentiment = SAD

    # 微博是否为转发微博
    weibo_multi_sentiment_bucket.Put(id_str, str(sentiment))  # 存储情感信息

print time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
