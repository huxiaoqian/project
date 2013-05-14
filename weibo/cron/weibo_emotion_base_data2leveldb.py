# -*- coding: utf-8 -*-

from xapian_weibo.xapian_backend import XapianSearch
from xapian_weibo.xapian_backend_extra import _load_weibos_from_xapian
from xapian_weibo.utils import load_emotion_words
import os
import leveldb
import datetime
import time
import opencc
import re

LEVELDBPATH = '/home/mirage/leveldb'
s = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_weibo')
cc = opencc.OpenCC('s2t')
emotions_words = load_emotion_words()
emotions_words = [unicode(e, 'utf-8') for e in emotions_words]
t_emotions_words = [cc.convert(e) for e in emotions_words]
emotions_words.extend(t_emotions_words)
emotions_words = [w.encode('utf-8') for w in emotions_words]
emotions_words_set = set(emotions_words)

weibo_is_retweet_status_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'lijun_weibo_is_retweet_status'),
                                                 block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
weibo_emoticoned_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'lijun_weibo_emoticoned'),
                                          block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
weibo_empty_retweet_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'lijun_weibo_empty_retweet'),
                                             block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))


def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print '%r %2.2f sec' % (method.__name__, te - ts)
        return result
    return timed


@timeit
def load_extra_weibos_from_xapian(ids):
    query_dict = {
        '$or': [],
    }
    for _id in ids:
        query_dict['$or'].append({'_id': _id})
    count, get_results = s.search(query=query_dict, fields=['_id', 'text'])
    print count
    return get_results


@timeit
def store2leveldb(get_results):
    empty_retweeted_ids = []

    count = 0
    ts = te = time.time()
    for r in get_results():
        id_str = str(r['_id'])

        # 微博是否为转发微博
        is_retweet_status = 1 if r['retweeted_status'] else 0
        weibo_is_retweet_status_bucket.Put(id_str, str(is_retweet_status))

        # 微博是否包含指定的表情符号集
        emotions = re.findall(r'\[(\S+?)\]', r['text'])
        is_emoticoned = 1 if set(emotions) & emotions_words_set else 0
        weibo_emoticoned_bucket.Put(id_str, str(is_emoticoned))

        # 是否为转发微博几个字
        is_empty_retweet = 1 if r['retweeted_status'] and r['text'] in ['转发微博', '轉發微博', 'Repost'] else 0
        weibo_empty_retweet_bucket.Put(id_str, str(is_empty_retweet))
        empty_retweeted_ids.append(r['retweeted_status'])

        count += 1
        if count % 100000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts)
            ts = te

    print 'empty_retweeted count', len(empty_retweeted_ids)
    return empty_retweeted_ids


@timeit
def store2leveldb_extra(get_results):
    count = 0
    ts = te = time.time()
    for r in get_results():
        id_str = str(r['_id'])

        # 微博是否包含指定的表情符号集
        emotions = re.findall(r'\[(\S+?)\]', r['text'])
        is_emoticoned = 1 if set(emotions) & emotions_words_set else 0
        weibo_emoticoned_bucket.Put(id_str, str(is_emoticoned))

        count += 1
        if count % 100000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts)
            ts = te


if __name__ == '__main__':
    get_results = _load_weibos_from_xapian()
    empty_retweeted_ids = store2leveldb(get_results)
    get_results = load_extra_weibos_from_xapian(empty_retweeted_ids)
    store2leveldb_extra(get_results)
