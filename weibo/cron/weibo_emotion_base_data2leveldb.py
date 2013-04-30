# -*- coding: utf-8 -*-

from xapian_weibo.xapian_backend import XapianSearch
from xapian_weibo.utils import load_emotion_words
import os
import leveldb
import datetime
import time
import opencc
import re

LEVELDBPATH = '/home/mirage/leveldb'
s = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline')


def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print '%r %2.2f sec' % (method.__name__, te - ts)
        return result
    return timed


@timeit
def load_weibos_from_xapian():
    total_days = 89
    today = datetime.datetime.today()
    end_ts = time.mktime(datetime.datetime(today.year, today.month, today.day, 2, 0).timetuple())
    begin_ts = end_ts - total_days * 24 * 3600

    query_dict = {
        'timestamp': {'$gt': begin_ts, '$lt': end_ts},
    }
    count, get_results = s.search(query=query_dict, fields=['id', 'retweeted_status', 'text'])
    print count
    return get_results


@timeit
def store2leveldb(get_results):
    cc = opencc.OpenCC('s2t')
    emotions_words = load_emotion_words()
    emotions_words = [unicode(e, 'utf-8') for e in emotions_words]
    t_emotions_words = [cc.convert(e) for e in emotions_words]
    emotions_words.extend(t_emotions_words)
    emotions_words_set = set(emotions_words)

    weibo_is_retweet_status_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'lijun_weibo_is_retweet_status'),
                                                     block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    weibo_emoticoned_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'lijun_weibo_emoticoned'),
                                              block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    weibo_empty_retweet_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'lijun_weibo_empty_retweet'),
                                                 block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

    count = 0
    ts = te = time.time()
    for r in get_results():
        id_str = str(r['id'])

        # 微博是否为转发微博
        is_retweet_status = 1 if r['retweeted_status'] else 0
        weibo_is_retweet_status_bucket.Put(id_str, str(is_retweet_status))

        # 微博是否包含指定的表情符号集
        emotions = re.findall(r'\[(\S+?)\]', r['text'])
        is_emoticoned = 1 if set(emotions) & emotions_words_set else 0
        weibo_emoticoned_bucket.Put(id_str, str(is_emoticoned))

        # 是否为转发微博几个字
        is_empty_retweet = 1 if r['text'] in [u'转发微博', u'轉發微博', u'Repost'] else 0
        weibo_empty_retweet_bucket.Put(id_str, str(is_empty_retweet))

        count += 1
        if count % 33333 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts)
            ts = te


if __name__ == '__main__':
    get_results = load_weibos_from_xapian()
    store2leveldb(get_results)
