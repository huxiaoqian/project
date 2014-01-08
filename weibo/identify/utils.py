# -*- coding: utf-8 -*-

import os
import random

#try:
from weibo.extensions import db
from weibo.model import TopicIdentification, KnowledgeList
#except ImportError:
#    print 'Warning: Not in web environment.'

from time_utils import ts2datetime, datetime2ts, window2time

from weibo.global_config import xapian_search_weibo as status_search, xapian_search_user as user_search


def acquire_topic_id(name):
    item = db.session.query(Topic).filter_by(topicName=name).first()
    if not item:
        #create a topic
        item = Topic(topicName=name)
        db.session.add(item)
        db.session.commit()
        db.session.refresh(item)
    return item.id

def acquire_topic_name(tid):
    item = db.session.query(Topic).filter_by(id=tid).first()
    if not item:
        return None
    return item.topicName

def acquire_user_by_id(identifyRange, uid):
    count, get_results = user_search.search(query={'_id': uid})
    # assert count==1, 'UID duplicated?'
    user = {}
    for r in get_results():
        user['name'] = r['name']
        user['location'] = r['location']
        if identifyRange == 'burst':
            #need user info index
            user['count1'] = random.randint(1, 10000)
            user['count2'] = random.randint(1, 10000)
        else:
            user['count1'] = r['followers_count']
            user['count2'] = r['friends_count']
    return user

def acquire_status_by_id(mid):
    count, get_results = status_search.search(query={'_id': mid})
    # assert count==1, 'MID duplicated?'
    status = None
    for r in get_results():
        status = r
    return status

def user_status(uid):
    old_items = db.session.query(KnowledgeList).filter(KnowledgeList.kID==uid).all()
    if len(old_items):
        return 1
    else:
        return 0

def is_in_trash_list(uid):
    return False

def read_rank_results(top_n, identifyRange, method, date, window, topic_id=None, compare=False):
    data = []
    if identifyRange == 'whole':
        items = db.session.query(WholeUserIdentification).filter_by(identifyMethod=method, identifyWindow=window, identifyDate=date).order_by(WholeUserIdentification.rank.asc()).limit(top_n)
    elif identifyRange == 'area':
        items = db.session.query(AreaUserIdentification).filter_by(topicId=topic_id, identifyMethod=method, identifyWindow=window, identifyDate=date).order_by(AreaUserIdentification.rank.asc()).limit(top_n)
    elif identifyRange == 'burst':
        items = db.session.query(BurstUserIdentification).filter_by(identifyMethod=method, identifyWindow=window, identifyDate=date).order_by(BurstUserIdentification.rank.asc()).limit(top_n)  
    else:
        return data
    if items.count():
        for item in items:
            rank = item.rank
            uid = item.userId
            user = acquire_user_by_id(identifyRange, uid)
            if not user:
                continue
            name = user['name']
            location = user['location']
            count1 = user['count1']
            count2 = user['count2']
            #read from external knowledge database
            status = user_status(uid)
            if compare:
                previous_rank = find_user_previous_rank(identifyRange, uid, date, method, window, topic_id)
                # comparison = rank_comparison(previous_rank, rank)
                comparison = rank - previous_rank
                row = (rank, uid, name, location, count1, count2, comparison, status)
            else:
                row = (rank, uid, name, location, count1, count2, status)
            data.append(row)
    return data

def save_rank_results(sorted_uids, identifyRange, method, date, window, topic_id=None):
    data = []
    rank = 1
    for uid in sorted_uids:
        user = acquire_user_by_id(identifyRange, uid)
        if not user:
            continue
        name = user['name']
        location = user['location']
        count1 = user['count1']
        count2 = user['count2']
        #read from external knowledge database
        status = user_status(uid)
        previous_rank = find_user_previous_rank(identifyRange, uid, date, method, window, topic_id)
        comparison = rank_comparison(previous_rank, rank)
        row = (rank, uid, name, location, count1, count2, comparison, status)
        data.append(row)
        if identifyRange == 'area':
            item = AreaUserIdentification(topicId=topic_id, rank=rank, userId=uid, identifyDate=date, identifyWindow=window, identifyMethod=method)
        elif identifyRange == 'whole':
            item = WholeUserIdentification(rank=rank, userId=uid, identifyDate=date, identifyWindow=window, identifyMethod=method)
        elif identifyRange == 'burst':
            item = BurstUserIdentification(rank=rank, userId=uid, identifyDate=date, identifyWindow=window, identifyMethod=method)
        else:
            break
        db.session.add(item)
        rank += 1
    db.session.commit()
    return data

def find_user_previous_rank(identifyRange, uid, date, method, window, topic_id):
    #read from previous window record
    base_time = datetime2ts(date)
    window_time = window2time(window)
    previous_date = ts2datetime(base_time-window_time)
    if identifyRange == 'area':
        item = db.session.query(AreaUserIdentification).filter_by(topicId=topic_id, userId=uid, identifyMethod=method, identifyWindow=window, identifyDate=previous_date).first()
    elif identifyRange == 'whole':
        item = db.session.query(WholeUserIdentification).filter_by(userId=uid, identifyMethod=method, identifyWindow=window, identifyDate=previous_date).first()
    elif identifyRange == 'burst':
        item = db.session.query(BurstUserIdentification).filter_by(userId=uid, identifyMethod=method, identifyWindow=window, identifyDate=previous_date).first()
    else:
        return None
    if item:
        return item.rank
    else:
        return None

def read_key_users(date, window, topic_id, top_n=10):
    items = db.session.query(AreaUserIdentification).filter_by(topicId=topic_id, identifyWindow=window, identifyDate=date).order_by(AreaUserIdentification.rank.asc()).limit(top_n)
    users = []
    if items.count():
        for item in items:
            uid = item.userId
            users.append(uid)
    return users

def rank_comparison(previous, current):
    if previous:
        if current-previous > 0:
            comparison = -1
        elif current-previous < 0:
            comparison = 1
        else:
            comparison = 0
    else:
        comparison = 1
    return comparison

def read_topic_rank_results(topic, top_n, method, date, window):
    data = []
    items = db.session.query(TopicIdentification).filter_by(topic=topic, identifyMethod=method, \
                                                            identifyWindow=window, identifyDate=date).order_by(TopicIdentification.rank.asc()).limit(top_n)
    if items.count():
        for item in items:
            rank = item.rank
            uid = item.userId
            user = acquire_user_by_id_v2(uid)
            if not user:
                continue
            name = user['name']
            location = user['location']
            count1 = user['count1']
            count2 = user['count2']
            #read from external knowledge database
            status = user_status(uid)
            row = (rank, uid, name, location, count1, count2, status)
            data.append(row)
    return data

def acquire_user_by_id_v2(uid):
    result = user_search.search_by_id(int(uid), fields=['name', 'location', 'followers_count', 'friends_count'])
    user = {}
    if result:
        user['name'] = result['name']
        user['location'] = result['location']
        user['count1'] = result['followers_count']
        user['count2'] = result['friends_count']
            
    return user
