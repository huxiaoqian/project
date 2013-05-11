# -*- coding: utf-8 -*-

from weibo.extensions import db

import weibo.model
from weibo.model import *

from time_utils import ts2datetime, datetime2ts, window2time

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

def read_rank_results(top_n, post_date, topic_id=None, r=None, m=None, w=None, compare=False):
    data = []
    items = db.session.query(UserIdentification).filter_by(topicId=topic_id, identifyRange=r, identifyMethod=m, identifyWindow=w, identifyDate=post_date).order_by(UserIdentification.rank.asc()).limit(top_n)
    if items.count():
        for item in items:
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
            status = user_status(uid)
            if compare:
                previous_rank = find_user_previous_rank(topic_id, uid, post_date, r=r, m=m, w=w)
                comparison = rank_comparison(previous_rank, rank)
                row = (rank, uid, name, location, followers, friends, comparison, status)
            else:
                row = (rank, uid, name, location, followers, friends, status)
            data.append(row)
    return data

def save_rank_results(sorted_uids, post_date=None, topic_id=None, r=None, m=None, w=None):
    data = []
    rank = 1
    for uid in sorted_uids:
        user = db.session.query(User).filter_by(id=uid).first()
        if not user:
            continue
        name = user.userName
        location = user.location
        followers = user.followersCount
        friends = user.friendsCount
        #read from external knowledge database
        status = user_status(uid)
        row = (rank, uid, name, location, followers, friends, status)
        data.append(row)
        item = UserIdentification(topicId=topic_id, rank=rank, userId=uid, identifyRange=r,identifyDate=post_date, identifyWindow=w, identifyMethod=m)
        db.session.add(item)
        rank += 1
    db.session.commit()
    return data

def find_user_previous_rank(topic_id, uid, post_date, r=None, m=None, w=None):
    #read from previous window record
    base_time = datetime2ts(post_date)
    window_time = window2time(w)
    previous_date = ts2datetime(base_time-window_time)
    item = db.session.query(UserIdentification).filter_by(topicId=topic_id, userId=uid, identifyRange=r, identifyMethod=m, identifyWindow=w, identifyDate=previous_date).first()
    if item:
        return item.rank
    else:
        return None

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

def user_status(uid):
    return 1
