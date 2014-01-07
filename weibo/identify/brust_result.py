#-*- coding: utf-8 -*-

import heapq
import random
import time
import os
import leveldb
try:
    from weibo.extensions import db
    from weibo.model import BurstIdentification
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

def burst_caculate(date, identifyWindow, identifyMethod, top_n):
    current_time = datetime2ts(date)
    current_date = ts2datetime(current_time)

    data = []
    if identifyWindow == 1:#时间窗为1，直接从数据库中读取
        items = db.session.query(BurstIdentification).filter((BurstIdentification.identifyMethod==identifyMethod)&(BurstIdentification.identifyWindow==identifyWindow)&(BurstIdentification.identifyDate==current_date)&(BurstIdentification.rank<=top_n)).all()
        if len(items):
            for item in items:
                rank = int(item.rank)
                uid = str(item.userId)
                followersCount = int(item.followersCount)
                activeCount = int(item.activeCount)
                importantCount = int(item.importantCount)
                activeDiff = int(item.activeDiff)
                importantDiff = int(item.importantDiff)
                
                user = acquire_user_by_id(uid)
                if user == 0:
                    continue
                name = user['name']
                location = user['location']
                friends_count = int(user['friends_count'])
                status = user_status(uid)
                if identifyMethod == 'active':
                    row = [rank, uid, name, location, friends_count, followersCount, activeCount, importantCount, activeDiff, status]
                else:
                    row = [rank, uid, name, location, friends_count, followersCount, activeCount, importantCount, importantDiff, status]
                data.append(row)
    else:#时间窗为7或30天
        end = datetime2ts(date)
        active = dict()
        important = dict()
        follower = dict()
        active_diff = dict()
        important_diff = dict()
        for i in range(0,identifyWindow):#遍历所有时间
            begints = end - i*24*3600
            begin = ts2datetime(begints)
            items = db.session.query(BurstIdentification).filter((BurstIdentification.identifyMethod==identifyMethod)&(BurstIdentification.identifyWindow==1)&(BurstIdentification.identifyDate==begin)).all()
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

                    if  active_diff.has_key(uid):
                        active_diff[uid] = active_diff[uid] + int(item.activeDiff)
                    else:
                        active_diff[uid] = int(item.activeDiff)

                    if  important_diff.has_key(uid):
                        important_diff[uid] = important_diff[uid] + int(item.importantDiff)
                    else:
                        important_diff[uid] = int(item.importantDiff)
                    
            else:
                continue
        rank_data = burst_rank(active,important,follower,active_diff,important_diff,top_n,identifyMethod)
        
        if identifyMethod == 'active':
            for i in range(0,len(rank_data)):
                rank = i+1
                uid = rank_data[i][5]
                followersCount = rank_data[i][4]
                activeCount = rank_data[i][2]
                importantCount = rank_data[i][3]
                activeDiff = rank_data[i][0]
                importantDiff = rank_data[i][1]

                user = acquire_user_by_id(uid)
                if user == 0:
                    continue
                name = user['name']
                location = user['location']
                friends_count = int(user['friends_count'])
                status = user_status(uid)

                row = [rank, uid, name, location, friends_count, followersCount, activeCount, importantCount, activeDiff, status]
                data.append(row)

        if identifyMethod == 'important':
            for i in range(0,len(rank_data)):
                rank = i+1
                uid = rank_data[i][5]
                followersCount = rank_data[i][4]
                activeCount = rank_data[i][2]
                importantCount = rank_data[i][3]
                activeDiff = rank_data[i][1]
                importantDiff = rank_data[i][0]

                user = acquire_user_by_id(uid)
                if user == 0:
                    continue
                name = user['name']
                location = user['location']
                friends_count = int(user['friends_count'])
                status = user_status(uid)

                row = [rank, uid, name, location, friends_count, followersCount, activeCount, importantCount, importantDiff, status]
                data.append(row)
            
    return data

def burst_rank(active,important,follower,active_diff,important_diff,top_n,identifyMethod):#突发排序

    if identifyMethod == 'active':
        print 'burst active rank'
        count = 0
        active_th = TopkHeap(top_n)
        for d,x in active_diff.items():
            count = count + 1
            if count%100 == 0:
                print count
            active_th.Push((x,important_diff[d],active[d],important[d],follower[d],d))#活跃度差值、重要度差值、活跃度、重要度、粉丝数、id
        
        return active_th.TopK()

    if identifyMethod == 'important':
        print 'burst important rank'
        count = 0
        important_th = TopkHeap(top_n)
        for d,x in important_diff.items():
            count = count + 1
            if count%100 == 0:
                print count
            important_th.Push((x,active_diff[d],active[d],important[d],follower[d],d))#重要度差值、活跃度差值、活跃度、重要度、粉丝数、id

        return important_th.TopK()

