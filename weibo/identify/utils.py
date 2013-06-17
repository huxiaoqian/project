# -*- coding: utf-8 -*-

import os
import random

from weibo.extensions import db

from weibo.model import Topic, WholeUserIdentification, AreaUserIdentification, BurstUserIdentification

from time_utils import ts2datetime, datetime2ts, window2time

from xapian_weibo.xapian_backend import XapianSearch

import scws

SCWS_ENCODING = 'utf-8'
SCWS_RULES = '/usr/local/scws/etc/rules.utf8.ini'
CHS_DICT_PATH = '/usr/local/scws/etc/dict.utf8.xdb'
CHT_DICT_PATH = '/usr/local/scws/etc/dict_cht.utf8.xdb'
IGNORE_PUNCTUATION = 1

ABSOLUTE_DICT_PATH = '/opt/xapian_weibo/dict/'
CUSTOM_DICT_PATH = os.path.join(ABSOLUTE_DICT_PATH, 'userdic.txt')
EXTRA_STOPWORD_PATH = os.path.join(ABSOLUTE_DICT_PATH, 'stopword.dic')
EXTRA_EMOTIONWORD_PATH = os.path.join(ABSOLUTE_DICT_PATH, 'emotionlist.txt')
EXTRA_ONE_WORD_WHITE_LIST_PATH = os.path.join(ABSOLUTE_DICT_PATH, 'one_word_white_list.txt')

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
    user_search = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_user', schema_version=1)
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
    status_search = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_weibo', schema_version=1)
    count, get_results = status_search.search(query={'_id': mid})
    # assert count==1, 'MID duplicated?'
    status = None
    for r in get_results():
        status = r
    return status

def user_status(uid):
    return 1

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
                comparison = rank_comparison(previous_rank, rank)
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

def load_scws():
    s = scws.Scws()
    s.set_charset(SCWS_ENCODING)

    s.set_dict(CHS_DICT_PATH, scws.XDICT_MEM)
    s.add_dict(CHT_DICT_PATH, scws.XDICT_MEM)
    s.add_dict(CUSTOM_DICT_PATH, scws.XDICT_TXT)

    # 把停用词全部拆成单字，再过滤掉单字，以达到去除停用词的目的
    s.add_dict(EXTRA_STOPWORD_PATH, scws.XDICT_TXT)
    # 即基于表情表对表情进行分词，必要的时候在返回结果处或后剔除
    s.add_dict(EXTRA_EMOTIONWORD_PATH, scws.XDICT_TXT)

    s.set_rules(SCWS_RULES)
    s.set_ignore(IGNORE_PUNCTUATION)
    return s

def cut(s, text, f=None):
    if f:
        tks = [token for token
               in s.participle(text)
               if token[1] in f]
    else:
        tks = [token for token
               in s.participle(text)]
    return [tk[0].decode('utf-8') for tk in tks]


