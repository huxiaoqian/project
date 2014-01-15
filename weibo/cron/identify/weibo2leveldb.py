# -*- coding: utf-8 -*-
import random
import time
import os
import leveldb
import redis
from time_utils import ts2datetime, datetime2ts
from xapian_weibo.xapian_backend import XapianSearch
path = '/home/mirage/dev/data/stub/master_timeline_weibo_'
LEVELDBPATH = '/home/mirage/leveldb'
field_daily_active_count_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'yuanshi_daily_user_followers'),
                                                  block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

USER_DOMAIN = "user_domain"
REDIS_HOST = '192.168.2.11'
REDIS_PORT = 6379

def getXapianWeiboByDate(datestr):
    # datestr: 20130908
    
    stub_file = path + datestr
    
    if os.path.exists(stub_file):
            xapian_search_weibo = XapianSearch(stub=stub_file, include_remote=True)
            return xapian_search_weibo
    else:
            return None

def _default_redis(host=REDIS_HOST, port=REDIS_PORT, db=0):
    return redis.StrictRedis(host, port, db)

def user2domain(uid):
    global_r0 = _default_redis()
    domainid = global_r0.hget(USER_DOMAIN, str(uid))
    if not domainid:
        domainid = -1 # not taged label
    
    return int(domainid)

def read_xapian(date):#读入数据

    datestr = date.replace('-', '')
    xapian_search_weibo = getXapianWeiboByDate(datestr)

    if not xapian_search_weibo:
        return 'wrong'
    get_results = xapian_search_weibo.iter_all_docs(fields=['user', 'reposts_count', 'comments_count', 'attitudes_count'])

    #计算活跃度、重要度和粉丝数
    active = dict()
    important = dict()
    follower = dict()
    domain = dict()
    n = 0
    yuan = 0
    for result in get_results:
        if not result['user']:
            continue
        if not result['reposts_count']:
            result['reposts_count'] = 0
        if not result['comments_count']:
            result['comments_count'] = 0
        if not result['attitudes_count']:
            result['attitudes_count'] = 0

        if  follower.has_key(result['user']):
            pass
        else:
            try:
                follower_count = field_daily_active_count_bucket.Get(str(result['user']))#查找对应用户的粉丝数
            except KeyError:
                continue
            follower[result['user']] = int(follower_count)
        
        n = n + 1
        if n%10000 == 0:
            print n
        if result['reposts_count'] == 0 and result['comments_count'] == 0:
            yuan = yuan + 1
        if  active.has_key(result['user']):
            active[result['user']] = active[result['user']] + 1
        else:
            active[result['user']] = 1

        if important.has_key(result['user']):
            important[result['user']] = important[result['user']] + result['reposts_count'] + result['comments_count'] + result['attitudes_count']
        else:
            important[result['user']] = result['reposts_count'] + result['comments_count'] + result['attitudes_count']

        if  domain.has_key(result['user']):
            pass
        else:
            area = user2domain(result['user'])
            domain[result['user']] = area
        
    #save2leveldb(active,important,follower,domain,datestr)
    print yuan
    return 'Done'

def save2leveldb(active,important,follower,domain,date):

    print 'save into leveldb'
    dailycount_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'yuanshi_daily_count_%s'%date),
                                                  block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))    
    count = 0
    for d,x in active.items():
        value = str(x)+'_'+str(important[d])+'_'+str(follower[d])+'_'+str(domain[d])
        count = count + 1
        if count%10000 == 0:
            print count
        dailycount_bucket.Put(str(d),value)

if __name__ == "__main__":

    answer = read_xapian('2013-09-01')
    print answer
