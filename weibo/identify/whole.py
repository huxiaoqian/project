# -*- coding: utf-8 -*-

from weibo.extensions import db
from weibo.model import *

from utils import save_rank_results
from time_utils import datetime2ts, window2time

def followers_rank(top_n, date, window_size):
    end_time = datetime2ts(date)
    start_time = end_time - window2time(window_size)
    users = db.session.query(User).order_by(User.followersCount.desc()).limit(top_n)
    sorted_uids = []
    for user in users:
        sorted_uids.append(user.id)
    data = save_rank_results(sorted_uids, 'whole', 'followers', date, window_size)
    return data

def active_rank(top_n, date, window_size):
    end_time = datetime2ts(date)
    start_time = end_time - window2time(window_size)
    users = db.session.query(User).order_by(User.statusesCount.desc()).limit(top_n)
    sorted_uids = []
    for user in users:
        sorted_uids.append(user.id)
    data = save_rank_results(sorted_uids, 'whole', 'active', date, window_size)
    return data

def important_rank(top_n, date, window_size):
    end_time = datetime2ts(date)
    start_time = end_time - window2time(window_size)
    users = db.session.query(User).order_by(User.followersCount.desc()).limit(top_n)
    sorted_uids = []
    for user in users:
        sorted_uids.append(user.id)
    data = save_rank_results(sorted_uids, 'whole', 'important', date, window_size)
    return data
