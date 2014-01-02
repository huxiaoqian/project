#-*- coding: utf-8 -*-

import heapq
import random
import time
import os
import leveldb
try:
    from config import db
    from model import WholeIdentification
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

def read_level(date):

    datestr = date.replace('-', '')

    whole_rank(datestr,date)
    return 'Done'

def whole_rank(datestr,date):#全网排序

    print 'whole rank'
    dailycount_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'yuanshi_daily_count_%s'%datestr),
                                                  block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    active_th = TopkHeap(1000)
    important_th = TopkHeap(1000)
    follower_th = TopkHeap(1000)
    count =0 
    for d,x in dailycount_bucket.RangeIter():
        count = count + 1
        if count%10000 == 0:
            print count
        active,important,follower,domain = x.split('_')
        d = int(d)
        active = int(active)
        important = int(important)
        follower = int(follower)
        domain = int(domain)

        active_th.Push((active,important,follower,d))#活跃度、重要度、粉丝数、id
        important_th.Push((important,active,follower,d))#重要度、活跃度、粉丝数、id
        follower_th.Push((follower,active,important,d))#粉丝数、活跃度、重要度、id

    active_data = active_th.TopK()
    print 'save active to mysql'
    for i in range(0,len(active_data)):        
        save_mysql(date,active_data[i][3],active_data[i][0],active_data[i][1],active_data[i][2],i+1,1,'active')

    important_data = important_th.TopK()
    print 'save important to mysql'
    for i in range(0,len(important_data)):        
        save_mysql(date,important_data[i][3],important_data[i][1],important_data[i][0],important_data[i][2],i+1,1,'important')

    follower_data = follower_th.TopK()
    print 'save follower to mysql'
    for i in range(0,len(follower_data)):        
        save_mysql(date,follower_data[i][3],follower_data[i][1],follower_data[i][2],follower_data[i][0],i+1,1,'followers')


def save_mysql(date,user_id,user_active,user_important,user_followers,user_rank,identifyWindow,identifyMethod):#时间、id、活跃度、重要度、粉丝数、排序、时间窗、排序标准
    current_time = datetime2ts(date)
    current_date = ts2datetime(current_time)

    #print user_rank, user_id, user_followers, user_active, user_important, current_date, identifyWindow, identifyMethod
    new_item = WholeIdentification(user_rank, user_id, user_followers, user_active, user_important, current_date, identifyWindow, identifyMethod)
    db.session.add(new_item)
    db.session.commit()

if __name__ == "__main__":

    answer = read_level('2013-09-02')
    print answer

    answer = read_level('2013-09-03')
    print answer

    answer = read_level('2013-09-04')
    print answer

    answer = read_level('2013-09-05')
    print answer

