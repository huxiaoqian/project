#-*- coding: utf-8 -*-

import heapq
import random
import time
try:
    from config import db
    from model import Topic, WholeUserIdentification, AreaUserIdentification, BurstUserIdentification, Whole
except ImportError:
    print 'Warning: Not in web environment.'
from global_config import xapian_search_user
from time_utils import ts2datetime, datetime2ts
 
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

def rank():

    get_results = xapian_search_user.iter_all_docs(fields=['_id', 'followers_count'])

    th = TopkHeap(500)
    count =0 
    for result in get_results:
        count = count + 1
##        if count>500:
##            break;
        if count%1000 == 0:
            print count
        i = {'id':result['_id'],'followers_count':result['followers_count']}
        th.Push(i)

    return th.TopK()

def save_mysql(date,user_id,user_followers,user_rank):
    current_time = datetime2ts(date)
    current_date = ts2datetime(current_time)

    new_item = WholeUserIdentification(rank=user_rank, userId=user_id, identifyDate=current_date, identifyWindow=1, identifyMethod='followers')
    #print type(new_item)
    db.session.add(new_item)
    db.session.commit()
    
    #print current_date,user_id,user_followers,user_rank
 
if __name__ == "__main__":
    
    data = rank()

    print 'save to mysql'
    count = 0 
    for i in range(0,len(data)):
        
        save_mysql('2013-12-25',data[i]['id'],data[i]['followers_count'],i+1)
        count = count + 1
        if count%100 == 0:
            print count 

