#-*- coding: utf-8 -*-

import heapq
import random
import time
import os
import leveldb
try:
    from config import db
    from model import AreaIdentification
except ImportError:
    print 'Warning: Not in web environment.'

from xapian_weibo.xapian_backend import XapianSearch
from time_utils import ts2datetime, datetime2ts
path = '/home/mirage/dev/data/stub/master_timeline_weibo_'
LEVELDBPATH = '/home/mirage/leveldb'

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

def read_level(date,area):

    datestr = date.replace('-', '')

    area_rank(datestr,date,area)
    return str(area)+' Done'

def area_rank(datestr,date,area):

    print 'area rank'
    dailycount_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'yuanshi_daily_count_%s'%datestr),
                                                  block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    active_th = TopkHeap(1000)
    important_th = TopkHeap(1000)
    follower_th = TopkHeap(1000)
    count =0 
    for d,x in dailycount_bucket.RangeIter():        
        active,important,follower,domain = x.split('_')
        d = int(d)
        active = int(active)
        important = int(important)
        follower = int(follower)
        domain = int(domain)
        if domain == area:
            count = count + 1
            if count%100 == 0:
                print count
            active_th.Push((active,important,follower,domain,d))#活跃度、重要度、粉丝数、领域、id
            important_th.Push((important,active,follower,domain,d))#重要度、活跃度、粉丝数、领域、id
            follower_th.Push((follower,active,important,domain,d))#粉丝数、活跃度、重要度、领域、id

    active_data = active_th.TopK()    
    if len(active_data):
        print 'save active to mysql'
        for i in range(0,len(active_data)):        
            save_mysql(date,active_data[i][4],active_data[i][0],active_data[i][1],active_data[i][2],active_data[i][3],i+1,1,'active')
    else:
        print str(area)+' is empty'

    important_data = important_th.TopK()
    if len(important_data):
        print 'save important to mysql'
        for i in range(0,len(important_data)):        
            save_mysql(date,important_data[i][4],important_data[i][1],important_data[i][0],important_data[i][2],active_data[i][3],i+1,1,'important')
    else:
        print str(area)+' is empty'

    follower_data = follower_th.TopK()
    if len(follower_data):
        print 'save follower to mysql'
        for i in range(0,len(follower_data)):        
            save_mysql(date,follower_data[i][4],follower_data[i][1],follower_data[i][2],follower_data[i][0],active_data[i][3],i+1,1,'followers')
    else:
        print str(area)+' is empty'

def save_mysql(date,user_id,user_active,user_important,user_followers,area,user_rank,identifyWindow,identifyMethod):#时间、id、活跃度、重要度、粉丝数、领域、排序、时间窗、排序标准
    current_time = datetime2ts(date)
    current_date = ts2datetime(current_time)

    #print user_rank, user_id, user_followers, user_active, user_important, area, current_date, identifyWindow, identifyMethod
    new_item = AreaIdentification(area, user_rank, user_id, user_followers, user_active, user_important, current_date, identifyWindow, identifyMethod)
    db.session.add(new_item)
    db.session.commit()

if __name__ == "__main__":

    for i in range(0,21):
        print str(i)+' starting...'
        answer = read_level('2013-09-02',i)
        print answer

    for i in range(0,21):
        print str(i)+' starting...'
        answer = read_level('2013-09-03',i)
        print answer

    for i in range(0,21):
        print str(i)+' starting...'
        answer = read_level('2013-09-04',i)
        print answer

    for i in range(0,21):
        print str(i)+' starting...'
        answer = read_level('2013-09-05',i)
        print answer

