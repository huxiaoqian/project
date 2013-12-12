# -*- coding: utf-8 -*-

import time, os, operator, datetime
from weibo.global_config import xapian_search_weibo, xapian_search_user
import numpy as np


ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print '%r %2.2f sec' % (method.__name__, te - ts)
        return result
    return timed
    

def getWeiboByMid(mid, emotion):
    weibo = xapian_search_weibo.search_by_id(int(mid), fields=['text', 'timestamp', 'user', 'retweeted_status', 'reposts_count'])
    if weibo:
        r = weibo
        uid = r['user']
        name = 'Unknown'
        text = r['text']
        reposts_count = r['reposts_count']
        retweeted_mid = r['retweeted_status']
        retweeted_text = 'None'
        if retweeted_mid:
            rweibo = xapian_search_weibo.search_by_id(int(retweeted_mid), fields=['text'])
            if rweibo:
                retweeted_text = rweibo['text']                
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
    user = xapian_search_user.search_by_id(int(uid), fields=['name'])
    if user:
        name = user['name']
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
