# -*- coding: utf-8 -*-

from xapian_weibo.xapian_backend import XapianSearch
from xapian_weibo.utils import timeit
from utils4scrapy.tk_maintain import _default_mongo
import os
import time
import datetime
import re
import opencc
import leveldb


def load_emotion_words():
    positive_emotions_words, negative_emotions_words = [], []
    with open('/home/mirage/clj/project/weibo/cron/new_seed_emoticons.txt') as f:
        for line in f:
            line = line.strip()
            word, emotion = line.split(':')
            if emotion == '1':
                positive_emotions_words.append(word)
            elif emotion == '2':
                negative_emotions_words.append(word)

    return positive_emotions_words, negative_emotions_words



LEVELDBPATH = '/home/mirage/leveldb'
s = XapianSearch(path='/opt/xapian_weibo/data/20130616/', name='master_timeline_weibo')
mongo = _default_mongo(usedb='master_timeline')
cc = opencc.OpenCC('s2t')
positive_emotions_words, negative_emotions_words = load_emotion_words()

#positive
positive_emotions_words = [unicode(e, 'utf-8') for e in positive_emotions_words]
t_positive_emotions_words = [cc.convert(e) for e in positive_emotions_words]
positive_emotions_words.extend(t_positive_emotions_words)
positive_emotions_words = [w.encode('utf-8') for w in positive_emotions_words]
positive_emotions_words_set = set(positive_emotions_words)
# negative
negative_emotions_words = [unicode(e, 'utf-8') for e in negative_emotions_words]
t_negative_emotions_words = [cc.convert(e) for e in negative_emotions_words]
negative_emotions_words.extend(t_negative_emotions_words)
negative_emotions_words = [w.encode('utf-8') for w in negative_emotions_words]
negative_emotions_words_set = set(negative_emotions_words)

weibo_emotion_positive_negative_global_count_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'lijun_weibo_emotion_positive_negative_global_count'),
                                                                      block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
weibo_emotion_positive_negative_male_count_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'lijun_weibo_emotion_positive_negative_male_count'),
                                                                    block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
weibo_emotion_positive_negative_female_count_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'lijun_weibo_emotion_positive_negative_female_count'),
                                                                      block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))


def load_140k_user_ids():
    with open('/home/mirage/data/2011_emotion_users_existed_20130615.txt') as f:
        lines = f.readlines()
    ids = [int(line.strip()) for line in lines]
    return ids


def load_user_genders_from_mongo(uids):
    user_genders = {}
    count = 0
    for uid in uids:
        user = mongo.master_timeline_user.find_one({'_id': uid})
        user_genders[uid] = user['gender']
        count += 1
        if count % 1000 == 0:
            print count

    print 'user genders data ready'
    return user_genders


def generate_timestamp():
    tss = []
    start_time = datetime.datetime(2011, 1, 1)
    step_time = datetime.timedelta(hours=2)
    while start_time < datetime.datetime(2013, 1, 1):
        tss.append(int(time.mktime(start_time.timetuple())))
        start_time += step_time
    return tss


@timeit
def load_extra_weibos_from_xapian(uids):
    begin_ts = time.mktime(datetime.datetime(2011, 1, 1).timetuple())
    end_ts = time.mktime(datetime.datetime(2013, 1, 1).timetuple())

    query_dict = {
        'timestamp': {'$gt': begin_ts, '$lt': end_ts},
        '$or': [],
    }
    for uid in uids:
        query_dict['$or'].append({'user': uid})
    count, get_results = s.search(query=query_dict, fields=['text', 'user', 'timestamp'], sort_by=['-timestamp'])
    print count
    return get_results


@timeit
def store2leveldb(get_results, user_genders, tss, global_count, male_count, female_count):
    ts_idx = 0
    timestamp = tss[ts_idx]
    for r in get_results():
        while r['timestamp'] < timestamp and ts_idx < len(tss) - 1:
            ts_idx += 1
            timestamp = tss[ts_idx]

        # 微博是否包含指定的表情符号集
        emotions = re.findall(r'\[(\S+?)\]', r['text'])
        emotions_set = set(emotions)
        include_positive = True if emotions_set & positive_emotions_words_set else False
        include_negative = True if emotions_set & negative_emotions_words_set else False
        if include_positive and include_negative:
            global_count[str(timestamp) + '_3'] += 1
            if user_genders[r['user']] == 'f':
                female_count[str(timestamp) + '_3'] += 1
            elif user_genders[r['user']] == 'm':
                male_count[str(timestamp) + '_3'] += 1

        elif include_positive and not include_negative:
            global_count[str(timestamp) + '_1'] += 1
            if user_genders[r['user']] == 'f':
                female_count[str(timestamp) + '_1'] += 1
            elif user_genders[r['user']] == 'm':
                male_count[str(timestamp) + '_1'] += 1

        elif not include_positive and include_negative:
            global_count[str(timestamp) + '_2'] += 1
            if user_genders[r['user']] == 'f':
                female_count[str(timestamp) + '_2'] += 1
            elif user_genders[r['user']] == 'm':
                male_count[str(timestamp) + '_2'] += 1


if __name__ == '__main__':
    user_ids_140k = load_140k_user_ids()
    user_ids_140k_set = set(user_ids_140k)
    user_genders = load_user_genders_from_mongo(user_ids_140k)
    tss = generate_timestamp()
    global_count, male_count, female_count = {}, {}, {}
    for ts in tss:
        for i in ['_1', '_2', '_3']:
            global_count[str(ts) + i] = 0
            male_count[str(ts) + i] = 0
            female_count[str(ts) + i] = 0

    size = 50
    for i in xrange(len(user_ids_140k) / size):
        print i * size
        ids = user_ids_140k[i * size: (i + 1) * size]
        get_results = load_extra_weibos_from_xapian(ids)
        store2leveldb(get_results, user_genders, tss, global_count, male_count, female_count)

    if len(user_ids_140k) % size:
        idx = len(user_ids_140k) / size
        ids = user_ids_140k[idx * size:]
        get_results = load_extra_weibos_from_xapian(ids)
        store2leveldb(get_results, user_genders, tss, global_count, male_count, female_count)

    for k, v in global_count.iteritems():
        weibo_emotion_positive_negative_global_count_bucket.Put(k, str(v))
    for k, v in male_count.iteritems():
        weibo_emotion_positive_negative_male_count_bucket.Put(k, str(v))
    for k, v in female_count.iteritems():
        weibo_emotion_positive_negative_female_count_bucket.Put(k, str(v))
