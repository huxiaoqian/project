# -*- coding: utf-8 -*-

import os
import time
import random

from config import db
from model import TopicStatus, TopicIdentification

from time_utils import ts2datetime, datetime2ts, window2time

from config import xapian_search_user as user_search


def acquire_topic_id(name, start_ts, end_ts, module="identify"):
    item = db.session.query(TopicStatus).filter_by(topic=name, start=start_ts, end=end_ts, module=module).first()
    if not item:
        #create a topic
        item = TopicStatus(module, -1, topic, start_ts, end_ts, 900, int(time.time()))
        db.session.add(item)
        db.session.commit()
        db.session.refresh(item)
    return item.id


def acquire_topic_name(tid, module='identify'):
    item = db.session.query(TopicStatus).filter_by(id=tid).first()
    if not item:
        return None
    return item.topic


def acquire_user_by_id(uid):
    result = user_search.search_by_id(int(uid), fields=['name', 'location'])
    user = {}
    if result:
        user['name'] = result['name']
        user['location'] = result['location']
        user['count1'] = r['followers_count']
        user['count2'] = r['friends_count']
            
    return user


def user_status(uid):
    return 1


def is_in_trash_list(uid):
    return False


def save_rank_results(sorted_uids, identifyRange, method, date, window, topicname):
    data = []
    rank = 1
    count = 0
    for uid in sorted_uids:
        user = acquire_user_by_id(uid)
        if not user:
            continue
        count = count + 1
        name = user['name']
        location = user['location']
        count1 = user['count1']
        count2 = user['count2']
        #read from external knowledge database
        status = user_status(uid)
        row = (rank, uid, name, location, count1, count2, status)
        data.append(row)
        if identifyRange == 'topic':
            item = TopicIdentification(topicname, rank, uid, date, window, method)
        else:
            break
        db.session.add(item)
        rank += 1
    db.session.commit()
    print 'done'
    return data


def read_key_users(date, window, topicname, top_n=10):
    items = db.session.query(TopicIdentification).filter_by(topic=topicname, identifyWindow=window, identifyDate=date).order_by(TopicIdentification.rank.asc()).limit(top_n)
    users = []
    if items.count():
        for item in items:
            uid = item.userId
            users.append(uid)
    return users
