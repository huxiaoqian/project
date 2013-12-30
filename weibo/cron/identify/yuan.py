#-*- coding: utf-8 -*-

import heapq
import random
import time
import os
import leveldb
try:
    from config import db
    from model import Topic, WholeIdentification, AreaUserIdentification, BurstUserIdentification, Whole
except ImportError:
    print 'Warning: Not in web environment.'
from global_config import xapian_search_user
from xapian_weibo.xapian_backend import XapianSearch
from time_utils import ts2datetime, datetime2ts
path = '/home/mirage/dev/data/stub/master_timeline_weibo_'
LEVELDBPATH = '/home/mirage/leveldb'
field_daily_active_count_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'yuanshi_daily_user_followers'),
                                                  block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))


def getXapianWeiboByDate(datestr):
    # datestr: 20130908
    
    stub_file = path + datestr
    
    if os.path.exists(stub_file):
            xapian_search_weibo = XapianSearch(stub=stub_file, include_remote=True)
            return xapian_search_weibo
    else:
            return None
 
class TopkHeap(object):
    def __init__(self, k):
        self.k = k
        self.data = []
 
    def Push(self, elem):
        if len(self.data) < self.k:
            heapq.heappush(self.data, elem)
        else:
            topk_small = self.data[0][0]
            if elem[0] > topk_small:
                heapq.heapreplace(self.data, elem)
 
    def TopK(self):
        return [x for x in reversed([heapq.heappop(self.data) for x in xrange(len(self.data))])]

def read_xapian(date):#读入数据

    begin_ts = datetime2ts(date)
    end_ts = begin_ts + 24*3600
    datestr = date.replace('-', '')
    xapian_search_weibo = getXapianWeiboByDate(datestr)

    if not xapian_search_weibo:
        return 'wrong'
    get_results = xapian_search_weibo.iter_all_docs(fields=['user', 'reposts_count', 'comments_count', 'attitudes_count'])

    #计算活跃度、重要度和粉丝数
    active = dict()
    important = dict()
    follower = dict()
    n = 0
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
        if n%1000 == 0:
            print n
        if  active.has_key(result['user']):
            active[result['user']] = active[result['user']] + 1
        else:
            active[result['user']] = 1

        if important.has_key(result['user']):
            important[result['user']] = important[result['user']] + result['reposts_count'] + result['comments_count'] + result['attitudes_count']
        else:
            important[result['user']] = result['reposts_count'] + result['comments_count'] + result['attitudes_count']                    
        
    active_rank(active,important,follower,date)
    important_rank(active,important,follower,date)
    follower_rank(active,important,follower,date)
    return 'right'

def active_rank(active,important,follower,date):#活跃度排序

    print 'active_rank'
    th = TopkHeap(500)
    count =0 
    for d,x in active.items():
        count = count + 1
        if count%1000 == 0:
            print count
        #i = {'id':d,'followers_count':x}
        th.Push((x,important[d],follower[d],d))#活跃度0、重要度1、粉丝数2、id3

    data = th.TopK()

    print 'save active to mysql'
    for i in range(0,len(data)):        
        save_mysql(date,data[i][3],data[i][0],data[i][1],data[i][2],i+1,1,'active')

def important_rank(active,important,follower,date):#重要度排序

    print 'important_rank'
    th = TopkHeap(500)
    count =0 
    for d,x in important.items():
        count = count + 1
        if count%1000 == 0:
            print count
        #i = {'id':d,'followers_count':x}
        th.Push((x,active[d],follower[d],d))#重要度0、活跃度1、粉丝数2、id3

    data = th.TopK()

    print 'save important to mysql'
    for i in range(0,len(data)):        
        save_mysql(date,data[i][3],data[i][1],data[i][0],data[i][2],i+1,1,'important')

def follower_rank(active,important,follower,date):#粉丝数排序    

    print 'follower_rank'
    th = TopkHeap(500)
    count =0 
    for d,x in follower.items():
        count = count + 1
        if count%1000 == 0:
            print count
        #i = {'id':d,'followers_count':x}
        th.Push((x,active[d],important[d],d))#粉丝数0、活跃度1、重要度2、id3

    data = th.TopK()

    print 'save follower to mysql'
    for i in range(0,len(data)):        
        save_mysql(date,data[i][3],data[i][1],data[i][2],data[i][0],i+1,1,'followers')

def save_mysql(date,user_id,user_active,user_important,user_followers,user_rank,identifyWindow,identifyMethod):#时间、id、活跃度、重要度、粉丝数、排序、时间窗、排序标准
    current_time = datetime2ts(date)
    current_date = ts2datetime(current_time)

    #print user_rank, user_id, user_followers, user_active, user_important, current_date, identifyWindow, identifyMethod
    new_item = WholeIdentification(user_rank, user_id, user_followers, user_active, user_important, current_date, identifyWindow, identifyMethod)
    db.session.add(new_item)
    db.session.commit()
 
if __name__ == "__main__":

    #followers_rank()
    answer = read_xapian('2013-09-01')
    print answer
    

