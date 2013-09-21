# -*- coding: utf-8 -*-

import time, os, operator, datetime
from xapian_weibo.utils import SimpleMapReduce, count_words
from xapian_weibo.xapian_backend import XapianSearch
import numpy as np

xapian_search_weibo = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_weibo', schema_version=2)
xapian_search_user = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_user', schema_version=1)

ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

def getStopWds(filename):
    swds = set()
    f = open(filename, 'r')
    count = 0
    for line in f.readlines():
        word = line.split()[0]
        swds.add(word)
        count += 1
    return swds
    
swds = getStopWds('./stpwds_linhao_20130826.txt')

def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print '%r %2.2f sec' % (method.__name__, te - ts)
        return result
    return timed


def top_keywords(get_results, top=1000):
    keywords_with_count = keywords(get_results)
    keywords_with_count.sort(key=operator.itemgetter(1))

    return keywords_with_count[len(keywords_with_count) - top:]


@timeit
def keywords(get_results):
    origin_data = []
    for r in get_results():
        origin_data.append(r['terms'].items())

    mapper = SimpleMapReduce(addcount2keywords, count_words)
    keywords_with_count = mapper(origin_data)

    return keywords_with_count


def addcount2keywords(origin_keywords_with_count):
    keywords_with_count = []
    for k, v in origin_keywords_with_count:
        if k not in swds:
            keywords_with_count.append((k, v))
    return keywords_with_count


def getWeiboByMid(mid, emotion):
    count, get_results = xapian_search_weibo.search(query={'_id': int(mid)}, fields=['text', 'timestamp', 'user', 'retweeted_status', 'reposts_count'])
    for r in get_results():
        uid = r['user']
        name = 'Unknown'
        text = r['text']
        reposts_count = r['reposts_count']
        retweeted_mid = r['retweeted_status']
        retweeted_text = 'None'
        if retweeted_mid:
            r_count, r_get_results = xapian_search_weibo.search(query={'_id': int(retweeted_mid)}, fields=['text'])
            for rr in r_get_results():
                retweeted_text = rr['text']
        ts = r['timestamp']
        date = datetime.date.fromtimestamp(int(ts)).isoformat()
        if getUsernameByUid(uid):
            name = getUsernameByUid(uid)
        user_link = "http://weibo.com/u/" + str(uid)
        weibo_link = weiboinfo2url(uid, mid)
        return [emotion, name, user_link, text, weibo_link, int(ts), reposts_count, retweeted_text]


def getUsernameByUid(uid):
    if not uid:
        return None
    count, get_results = xapian_search_user.search(query={'_id': int(uid)}, fields=['name'])
    for r in get_results():
        name = r['name']
        return name
    return None


def weiboinfo2url(uid, mid):
    return "http://weibo.com/{uid}/{mid}".format(uid=uid, mid=mid_to_str(mid))


def mid_to_str(mid):
    mid = str(mid)
    id1 = mid[0: 2]
    id2 = mid[2: 9]
    id3 = mid[9: 16]
    id_list = [id1, id2, id3]
    id_list = [base62_encode(int(mid)) for mid in id_list]
    return "".join(map(str, id_list))


def base62_encode(num, alphabet=ALPHABET):
    """Encode a number in Base X

    `num`: The number to encode
    `alphabet`: The alphabet to use for encoding
    """
    if (num == 0):
        return alphabet[0]
    arr = []
    base = len(alphabet)
    while num:
        rem = num % base
        num = num // base
        arr.append(alphabet[rem])
    arr.reverse()
    return ''.join(arr)


def st_variation(lis1, lis2, lis3):
    ave = np.mean(lis1)    
    variation1 = [np.abs(num - ave)/ave for num in lis1]
    ave = np.mean(lis2)
    variation2 = [np.abs(num - ave)/ave for num in lis2]
    ave = np.mean(lis3)
    variation3 = [np.abs(num - ave)/ave for num in lis3]
    variation = [variation1[cursor]+variation2[cursor]+variation3[cursor] for cursor in range(len(lis1))]
    return variation


def find_topN(lis,n):
    new = [lis[0]]
    rank = [0]
    num_cursor = 1
    for num in lis[1:]:
        num_cursor += 1
        find = 0
        cursor = 0
        if num > new[0]:
            new[0:0] = [num]
            rank[0:0] = [num_cursor-1]
        else:
            for i in new:
                if num > i:
                    new[cursor:cursor] = [num]
                    rank[cursor:cursor] = [num_cursor-1]
                    find = 1
                    break
                cursor += 1
            if find == 0:
                new.append(num)
                rank.append(num_cursor-1)
            
    peak_x = []
    peak_y = []
    cursor = 0
    for y in new:
        if rank[cursor]!=0 and rank[cursor]!=len(new)-1:
            if y > lis[rank[cursor]+1] and y > lis[rank[cursor]-1]:
                peak_x.append(rank[cursor])
                peak_y.append(y)

        elif rank[cursor]==0:
            if y > lis[rank[cursor]+1]:
                peak_x.append(rank[cursor])
                peak_y.append(y)
        elif rank[cursor]==rank[cursor]!=len(new)-1:
            if y > lis[rank[cursor]+1]:
                peak_x.append(rank[cursor])
                peak_y.append(y)
        if len(peak_x)==n:
            break
        cursor += 1
    return peak_x[:n],peak_y[:n]


if __name__ == '__main__':
    print os.getcwd()