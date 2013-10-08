# -*- coding: utf-8 -*-

import os
import random


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

status_search = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_weibo', schema_version=2)
user_search = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_user', schema_version=1)

topic_name_id_dict = {'1': '春节', '2': '两会'}#加入50个人的字典

def acquire_topic_name(id):
    try:
        name = topic_name_id_dict[str(id)]
        return name
    except:
        return None
    
def acquire_topic_id(name):
    try:
        for k, v in topic_name_id_dict.iteritems():
            if name == v:
                return k
        return id
    except:
        return None
        
    

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
    return 1

def is_in_trash_list(uid):
    return False

def read_rank_results(top_n, identifyRange, method, date, window, topic_id=None, compare=False):
    data = []

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
        rank += 1

    return data


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
