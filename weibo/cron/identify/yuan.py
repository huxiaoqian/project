#-*- coding: utf-8 -*-

import heapq
import random
import time
import os
try:
    from config import db
    from model import Topic, WholeUserIdentification, AreaUserIdentification, BurstUserIdentification, Whole
except ImportError:
    print 'Warning: Not in web environment.'
from global_config import xapian_search_user
from xapian_weibo.xapian_backend import XapianSearch
from time_utils import ts2datetime, datetime2ts
path = '/home/mirage/dev/data/stub/master_timeline_weibo_'


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
            topk_small = self.data[0]['followers_count']
            if elem['followers_count'] > topk_small:
                heapq.heapreplace(self.data, elem)
 
    def TopK(self):
        return [x for x in reversed([heapq.heappop(self.data) for x in xrange(len(self.data))])]

def followers_rank():#粉丝数排序

    get_results = xapian_search_user.iter_all_docs(fields=['_id', 'followers_count'])

    th = TopkHeap(500)
    count =0 
    for result in get_results:
        count = count + 1
        if count%1000 == 0:
            print count
        i = {'id':result['_id'],'followers_count':result['followers_count']}
        th.Push(i)

    data = th.TopK()

    print 'save to mysql'
    count = 0 
    for i in range(0,len(data)):        
        save_followers('2013-12-25',data[i]['id'],data[i]['followers_count'],i+1)
        count = count + 1
        if count%100 == 0:
            print count

def save_followers(date,user_id,user_followers,user_rank):
    current_time = datetime2ts(date)
    current_date = ts2datetime(current_time)

    new_item = Whole(rank=user_rank, userId=user_id, count=user_followers, identifyDate=current_date, identifyWindow=1, identifyMethod='followers')
    db.session.add(new_item)
    db.session.commit()

def read_xapian(date):#读入数据

    begin_ts = datetime2ts(date)
    end_ts = begin_ts + 24*3600
    datestr = date.replace('-', '')
    xapian_search_weibo = getXapianWeiboByDate(datestr)

    if not xapian_search_weibo:
        return 'wrong'
    get_results = xapian_search_weibo.iter_all_docs(fields=['user', 'reposts_count', 'comments_count', 'attitudes_count'])

    #计算活跃度和重要度
    active = dict()
    important = dict()
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
        
    active_rank(active,date)
    important_rank(important,date)

    return 'right'

def active_rank(get_results,date):#活跃度排序

    print 'active_rank'
    th = TopkHeap(500)
    count =0 
    for d,x in get_results.items():
        count = count + 1
        if count%1000 == 0:
            print count
        i = {'id':d,'followers_count':x}
        th.Push(i)

    data = th.TopK()

    print 'save active to mysql'
    count = 0 
    for i in range(0,len(data)):        
        save_active(date,data[i]['id'],data[i]['followers_count'],i+1)
        count = count + 1
        if count%100 == 0:
            print count

def save_active(date,user_id,user_followers,user_rank):
    current_time = datetime2ts(date)
    current_date = ts2datetime(current_time)

    #print user_rank, user_id, user_followers, current_date
    new_item = Whole(rank=user_rank, userId=user_id, count=user_followers, identifyDate=current_date, identifyWindow=1, identifyMethod='active')
    db.session.add(new_item)
    db.session.commit()

def important_rank(get_results,date):#重要度排序

    print 'important_rank'
    th = TopkHeap(500)
    count =0 
    for d,x in get_results.items():
        count = count + 1
        if count%1000 == 0:
            print count
        i = {'id':d,'followers_count':x}
        th.Push(i)

    data = th.TopK()

    print 'save important to mysql'
    count = 0 
    for i in range(0,len(data)):        
        save_important(date,data[i]['id'],data[i]['followers_count'],i+1)
        count = count + 1
        if count%100 == 0:
            print count

def save_important(date,user_id,user_followers,user_rank):
    current_time = datetime2ts(date)
    current_date = ts2datetime(current_time)

    #print user_rank, user_id, user_followers, current_date
    new_item = Whole(rank=user_rank, userId=user_id, count=user_followers, identifyDate=current_date, identifyWindow=1, identifyMethod='important')
    db.session.add(new_item)
    db.session.commit()

 
if __name__ == "__main__":

    #followers_rank()
    answer = read_xapian('2013-09-01')
    print answer
    

