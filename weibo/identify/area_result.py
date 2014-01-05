#-*- coding: utf-8 -*-

import heapq
import random
import time
import os
import leveldb
try:
    from weibo.extensions import db
    from weibo.model import AreaIdentification
except ImportError:
    print 'Warning: Not in web environment.'

from time_utils import ts2datetime, datetime2ts, window2time
from weibo.global_config import xapian_search_user as user_search

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

def acquire_user_by_id(uid):
    count, get_results = user_search.search(query={'_id': uid})
    user = {}
    if count == 0:
        return 0
    for r in get_results():
        user['name'] = r['name']
        user['location'] = r['location']
        user['friends_count'] = r['friends_count']
    return user

def user_status(uid):
    return 1

def area_caculate(date, identifyWindow, identifyMethod, top_n, area):
    current_time = datetime2ts(date)
    current_date = ts2datetime(current_time)

    data = []
    if identifyWindow == 1:#时间窗为1，直接从数据库中读取
        items = db.session.query(AreaIdentification).filter((AreaIdentification.identifyMethod==identifyMethod)&(AreaIdentification.identifyWindow==identifyWindow)&(AreaIdentification.identifyDate==current_date)&(AreaIdentification.rank<=top_n)&(AreaIdentification.topicId==area)).all()
        if len(items):
            for item in items:
                rank = int(item.rank)
                uid = str(item.userId)
                followersCount = int(item.followersCount)
                activeCount = int(item.activeCount)
                importantCount = int(item.importantCount)

                user = acquire_user_by_id(uid)
                if user == 0:
                    continue
                name = user['name']
                location = user['location']
                friends_count = int(user['friends_count'])
                status = user_status(uid)

                row = [rank, uid, name, location, friends_count, followersCount, activeCount, importantCount, status]
                data.append(row)
    else:#时间窗为7或30天
        end = datetime2ts(date)
        active = dict()
        important = dict()
        follower = dict()
        for i in range(0,identifyWindow):#遍历所有时间
            begints = end - i*24*3600
            begin = ts2datetime(begints)
            items = db.session.query(AreaIdentification).filter((AreaIdentification.identifyMethod==identifyMethod)&(AreaIdentification.identifyWindow==1)&(AreaIdentification.identifyDate==begin)&(AreaIdentification.topicId==area)).all()
            if len(items):
                for item in items:#将数据库中粉丝数、活跃度、重要度累加
                    uid = str(item.userId)
                    if  follower.has_key(uid):
                        pass
                    else:
                        count, get_results = user_search.search(query={'_id': uid},field=['followers_count'])#查找对应用户的粉丝数
                        if count == 0:
                            continue
                        for r in get_results():
                            follower[uid] = int(r['followers_count'])

                    if  active.has_key(uid):
                        active[uid] = active[uid] + int(item.activeCount)
                    else:
                        active[uid] = int(item.activeCount)

                    if  important.has_key(uid):
                        important[uid] = important[uid] + int(item.importantCount)
                    else:
                        important[uid] = int(item.importantCount)
                    
            else:
                continue
        rank_data = area_rank(active,important,follower,top_n,identifyMethod)
        
        if identifyMethod == 'active':
            for i in range(0,len(rank_data)):
                rank = i+1
                uid = rank_data[i][3]
                followersCount = rank_data[i][2]
                activeCount = rank_data[i][0]
                importantCount = rank_data[i][1]

                user = acquire_user_by_id(uid)
                if user == 0:
                    continue
                name = user['name']
                location = user['location']
                friends_count = int(user['friends_count'])
                status = user_status(uid)

                row = [rank, uid, name, location, friends_count, followersCount, activeCount, importantCount, status]
                data.append(row)

        if identifyMethod == 'important':
            for i in range(0,len(rank_data)):
                rank = i+1
                uid = rank_data[i][3]
                followersCount = rank_data[i][2]
                activeCount = rank_data[i][1]
                importantCount = rank_data[i][0]

                user = acquire_user_by_id(uid)
                if user == 0:
                    continue
                name = user['name']
                location = user['location']
                friends_count = int(user['friends_count'])
                status = user_status(uid)

                row = [rank, uid, name, location, friends_count, followersCount, activeCount, importantCount, status]
                data.append(row)

        if identifyMethod == 'followers':
            for i in range(0,len(rank_data)):
                rank = i+1
                uid = rank_data[i][3]
                followersCount = rank_data[i][0]
                activeCount = rank_data[i][1]
                importantCount = rank_data[i][2]

                user = acquire_user_by_id(uid)
                if user == 0:
                    continue
                name = user['name']
                location = user['location']
                friends_count = int(user['friends_count'])
                status = user_status(uid)

                row = [rank, uid, name, location, friends_count, followersCount, activeCount, importantCount, status]
                data.append(row)
            
    return data

def area_rank(active,important,follower,top_n,identifyMethod):#领域排序

    if identifyMethod == 'active':
        print 'area active rank'
        count = 0
        active_th = TopkHeap(top_n)
        for d,x in active.items():
            count = count + 1
            if count%100 == 0:
                print count
            active_th.Push((x,important[d],follower[d],d))#活跃度、重要度、粉丝数、id
        
        return active_th.TopK()

    if identifyMethod == 'important':
        print 'area important rank'
        count = 0
        important_th = TopkHeap(top_n)
        for d,x in important.items():
            count = count + 1
            if count%100 == 0:
                print count
            important_th.Push((x,active[d],follower[d],d))#重要度、活跃度、粉丝数、id

        return important_th.TopK()

    if identifyMethod == 'followers':
        print 'area followers rank'
        count = 0
        follower_th = TopkHeap(top_n)
        for d,x in follower.items():
            count = count + 1
            if count%100 == 0:
                print count
            follower_th.Push((x,active[d],important[d],d))#粉丝数、活跃度、重要度、id

        return follower_th.TopK()

