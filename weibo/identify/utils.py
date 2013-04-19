# -*- coding: utf-8 -*-

import time
import tempfile

from weibo.extensions import db

import weibo.model
from weibo.model import *

from time_utils import ts2datetime, window2time
from demo_utils import demo_results
from hadoop_utils import hadoop_results

def acquire_id(name, value):
    if not value:
        return -1
    m = getattr(weibo.model, name)
    item = db.session.query(m).filter_by(**{'%sName' % name.lower(): value}).first()
    if item:
        return item.id
    else:
        item = m(**{'%sName' % name.lower(): value})
        db.session.add(item)
        db.session.commit()
        db.session.refresh(item)
        return item.id

def acquire_value(name, id):
    if id < 0:
        return None
    m = getattr(weibo.model, name)
    item = db.session.query(m).filter_by(**{'id': id}).first()
    if item:
        return getattr(item, '%sName' % name.lower(), None)
    else:
        return None

def read_previous_results(topic_id, top_n, r='area', m='PageRank', w=1):
    data = []
    #change to seconds
    window_time = window2time(w)
    previous_date = ts2datetime(time.time()-window_time)
    items = db.session.query(UserIdentification).filter_by(topicId=topic_id, identifyRange=r, identifyMethod=m, identifyWindow=w, identifyDate=previous_date).order_by(UserIdentification.rank.asc())
    if items.count():
        count = 0
        for item in items:
            if count > top_n:
                break
            rank = item.rank
            uid = item.userId
            user = db.session.query(User).filter_by(id=uid).first()
            if not user:
                continue
            name = user.userName
            location = user.location
            followers = user.followersCount
            friends = user.friendsCount
            #read from external knowledge database
            status = 1
            row = (rank, uid, name, location, followers, friends, status)
            data.append(row)
            count += 1
    return data

def read_current_results(topic_id, top_n, r='area', m='PageRank', w=1, demo=False):
    data = []
    current_date = ts2datetime(time.time())
    items = db.session.query(UserIdentification).filter_by(topicId=topic_id, identifyRange=r, identifyMethod=m, identifyWindow=w, identifyDate=current_date).order_by(UserIdentification.rank.asc())
    if items.count():
        count = 0
        for item in items:
            if count > top_n:
                break
            rank = item.rank
            user = item.user
            uid = user.id
            name = user.userName
            location = user.location
            followers = user.followersCount
            friends = user.friendsCount
            #read from external knowledge database
            status = 1
            previous_rank = find_user_previous_rank(topic_id, uid, r=r, m=m, w=w)
            comparison = rank_comparison(previous_rank, rank)
            row = (rank, uid, name, location, followers, friends, comparison, status)
            data.append(row)
            count += 1
    else:
        if demo:
            sorted_pr = demo_results(topic_id, top_n, r, m, w)
        else:
            sorted_pr = hadoop_results(topic_id, top_n)
            if sorted_pr == 'results_not_prepared':
                return sorted_pr

        rank = 1
        for uid, pr in sorted_pr:
            user = db.session.query(User).filter_by(id=uid).first()
            if not user:
                continue
            name = user.userName
            location = user.location
            followers = user.followersCount
            friends = user.friendsCount
            #read from external knowledge database
            status = 1
            previous_rank = find_user_previous_rank(topic_id, uid, r=r, m=m, w=w)
            comparison = rank_comparison(previous_rank, rank)
            row = (rank, uid, name, location, followers, friends, comparison, status)
            data.append(row)
            item = UserIdentification(topicId=topic_id, rank=rank, userId=uid, identifyRange='area',identifyDate=current_date, identifyWindow=1, identifyMethod='PageRank')
            db.session.add(item)
            rank += 1
        db.session.commit()

    return data

def rank_comparison(previous, current):
    if previous:
        if current-previous > 0:
            comparison = 1
        elif current-previous < 0:
            comparison = -1
        else:
            comparison = 0
    else:
        comparison = 1
    return comparison

def find_user_previous_rank(topic_id, uid, r='area', m='PageRank', w=1):
    #read from previous window record
    window_time = window2time(w)
    previous_date = ts2datetime(time.time()-window_time)
    item = db.session.query(UserIdentification).filter_by(topicId=topic_id, userId=uid, identifyRange=r, identifyMethod=m, identifyWindow=w, identifyDate=previous_date).first()
    if item:
        return item.rank
    else:
        return None
