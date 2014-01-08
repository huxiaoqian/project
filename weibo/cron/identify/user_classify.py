#-*-coding:utf-8-*-
'''用户领域分类模型
'''

import os
import sys
import time
import redis
import leveldb
from time_utils import datetimestr2ts
from operator import itemgetter, attrgetter
from dynamic_xapian_weibo import getXapianWeiboByDuration
from config import xapian_search_user, LEVELDBPATH, fields_value, \
                   labels, REDIS_PORT, REDIS_HOST

try:
    simple_user_field_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'spiedusers'),
                                               block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    global_user_field_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_global_user_field_20131228'),
                                               block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
except:
    print 'leveldb not available'
mbr = {"culture":0, "entertainment":0, "fashion":0,'education':0,"finance":0, "sports":0, "technology":0,'media':0}
DOMAIN_LIST = fields_value + labels
GLOBAL_USER_AREA_COUNT = "global_user_area_count:%s" # area


def _default_redis(host=REDIS_HOST, port=REDIS_PORT, db=0):
    return redis.StrictRedis(host, port, db)


def readProtoUser():
    f = open("protou.txt", "r")
    protou={}
    for line in f:
        area = line.split(":")[0]
        for u in (line.split(":")[1]).split():
            protou[int(u)] = area

    return protou


def readProtoWord():
    f = open("protow.txt", "r")
    protow={}
    for line in f:
        area = line.split(":")[0]
        words = line.split(":")[1].split(',')
        for word in words:
            try:
                areas = protow[word]
                areas.extend(area)
                protow[word] = areas
            except KeyError:
                protow[word] = [area]
    
    return protow


def getUserFriends(uid):
    friends = []

    user = xapian_search_user.search_by_id(int(uid), fields=['friends'])
    if user:
        friends = user['friends']

    return friends


def calFieldByFriends():
    protousers = readProtoUser()

    iter_count = 0
    ts = te = time.time()
    users = xapian_search_user.iter_all_docs(fields=['friends'])
    for user in users:
        area_dict = {}
        friends = user['friends']

        for fri in friends:
            try:
                area = protousers[fri]
                area_dict[area] += 1
            except KeyError:
                pass

        if area_dict != {}:
            area_counts = sorted(area_dict.iteritems(), key=itemgetter(1), reverse=True)
            if len(area_counts) == 1:
                areas = area_counts[0][0]
            else:
                areas = area_counts[0][0] + ',' + area_counts[1][0]
            
            try:             
                e_areas = global_user_field_bucket.Get(str(uid) + '_' + update_datestr)
                areas = ','.join(e_areas.split(',') + areas.split(','))
            except KeyError:
                pass

            global_user_field_bucket.Put(str(uid) + '_' + update_datestr, areas)                

        if iter_count % 10000 == 0:
            te = time.time()
            print iter_count, '%s sec' % (te - ts)
            ts = te

        iter_count += 1


def calFieldByWords():
    protowords = readProtoWord()
    weibos = xapian_search_weibo.iter_all_docs(fields=['terms', 'user'])

    iter_count = 0
    ts = te = time.time()
    for weibo in weibos:
        terms = weibo['terms']
        user = weibo['user']
        areas_list = []

        for term in terms:
            try:
                areas_list.extend(protowords[term])
            except KeyError:
                pass

        for area in areas_list:
            r.zadd(GLOBAL_USER_AREA_COUNT % area, 1, user)

        if iter_count % 10000 == 0:
            te = time.time()
            print iter_count, '%s sec' % (te - ts)
            ts = te

        iter_count += 1


if __name__ == '__main__':
    r = _default_redis()
    update_datestr = '20131228'
    datestr_list = ['20130901', '20130902', '20130903', '20130904', '20130905']
    xapian_search_weibo = getXapianWeiboByDuration(datestr_list)
    #calFieldByFriends()

    calFieldByWords()