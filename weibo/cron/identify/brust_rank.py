#-*- coding: utf-8 -*-

import heapq
import random
import time
import os
import leveldb
try:
    from config import db
    from model import BurstIdentification
except ImportError:
    print 'Warning: Not in web environment.'
from config import LEVELDBPATH
from time_utils import ts2datetime, datetime2ts


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

    end = datetime2ts(date)
    begints = end - 24*3600
    begin = ts2datetime(begints)
    
    burst_rank(begin, date, date)
    return 'Done'

def burst_rank(begin_time,end_time,date):
    print 'burst rank'
    dailycount_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'yuanshi_daily_count_%s'%end_time),
                                                  block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    pre_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'yuanshi_daily_count_%s'%begin_time),
                                                  block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
    active_th = TopkHeap(1000)
    important_th = TopkHeap(1000)
    count =0
    te = ts = time.time()
    for d,x in dailycount_bucket.RangeIter():
        if count % 10000 == 0:
            te = time.time()
            print count, '%s sec' % (te - ts), 'identify burst rank to mysql ', date
            ts = te
        count = count + 1

        active,important,follower,domain = x.split('_')
        d = int(d)
        active = int(active)
        important = int(important)
        follower = int(follower)
        domain = int(domain)

        try:
            pre_result = pre_bucket.Get(str(d))#查找前一天对应用户的参数
        except KeyError:
            pre_result = '0_0_0_-1'
        pre_active,pre_important,pre_follower,pre_domain = pre_result.split('_')
        pre_active = int(pre_active)
        pre_important = int(pre_important)
        pre_follower = int(pre_follower)
        pre_domain = int(pre_domain)

        active_diff = active - pre_active
        important_diff = important - pre_important
        
        active_th.Push((active_diff,important_diff,active,important,follower,d))#活跃度差值、重要度差值、活跃度、重要度、粉丝数、id
        important_th.Push((important_diff,active_diff,active,important,follower,d))#重要度差值、活跃度差值、活跃度、重要度、粉丝数、id

    active_data = active_th.TopK()
    print 'save active to mysql'
    for i in range(0,len(active_data)):        
        save_mysql(date,active_data[i][5],active_data[i][0],active_data[i][1],active_data[i][2],active_data[i][3],active_data[i][4],i+1,1,'active')

    important_data = important_th.TopK()
    print 'save important to mysql'
    for i in range(0,len(important_data)):        
        save_mysql(date,important_data[i][5],important_data[i][1],important_data[i][0],important_data[i][2],important_data[i][3],important_data[i][4],i+1,1,'important')

def save_mysql(date,user_id,active_diff,important_diff,user_active,user_important,user_followers,user_rank,identifyWindow,identifyMethod):
    #时间、id、活跃度差值、重要度差值、活跃度、重要度、粉丝数、排序、时间窗、排序标准
    current_time = datetime2ts(date)
    current_date = ts2datetime(current_time)

    #print user_rank, user_id, user_followers, user_active, user_important, active_diff, important_diff, current_date, identifyWindow, identifyMethod
    new_item = BurstIdentification(user_rank, user_id, user_followers, user_active, user_important, active_diff, important_diff, current_date, identifyWindow, identifyMethod)
    db.session.add(new_item)
    db.session.commit()

if __name__ == "__main__":
    import sys
    date = sys.argv[1] # '2013-09-03'
    answer = read_level(date)
    print answer
