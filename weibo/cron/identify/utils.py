# -*- coding: utf-8 -*-

import os
import time
import random

from config import db, ELEVATOR_HOST, ELEVATOR_PORT
from model import TopicStatus, TopicIdentification, TopicGexf

from time_utils import ts2datetime, datetime2ts, window2time

from config import xapian_search_user as user_search

from pyelevator import WriteBatch, Elevator

def _default_elevator(db_name='default'):
    db = Elevator(db_name, transport='tcp', endpoint='%s:%s' % (ELEVATOR_HOST, ELEVATOR_PORT))
    return db

def init_db():
    E.createdb('linhao_identify_gexf')

    E.disconnect()


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
    result = user_search.search_by_id(int(uid), fields=['name', 'location', 'followers_count', 'friends_count'])
    user = {}
    if result:
        user['name'] = result['name']
        user['location'] = result['location']
        user['count1'] = result['followers_count']
        user['count2'] = result['friends_count']
            
    return user


def user_status(uid):
    return 1


def is_in_trash_list(uid):
    return False


def save_rank_results(sorted_uids, identifyRange, method, date, window, topicname):
    data = []
    rank = 1
    count = 0

    exist_items = db.session.query(TopicIdentification).filter(TopicIdentification.topic==topicname, \
                                                               TopicIdentification.identifyWindow==window, \
                                                               TopicIdentification.identifyDate==date, \
                                                               TopicIdentification.identifyMethod==method).all()
    for item in exist_items:
        db.session.delete(item)
    db.session.commit()

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


def _utf8_unicode(s):
    if isinstance(s, unicode):
        return s
    else:
        return unicode(s, 'utf-8')


def save_gexf_results(topic, identifyDate, identifyWindow, identifyGexf):
    E = _default_elevator()
    E.connect('linhao_identify_gexf')
    key = _utf8_unicode(topic) + '_' + str(identifyDate) + '_' + str(identifyWindow)
    value = str(identifyGexf)
    E.Put(key, value)
    E.disconnect()


if __name__ == '__main__':
    E = _default_elevator()
    # init_db()
