# -*- coding: utf-8 -*-

from datetime import datetime

from config import db

from model import WholeUserIdentification, BurstUserIdentification

def get_leveldb(method, ts):
    date = datetime.fromtimestamp(ts)
    db_name = 'hanyang_daily_user_%s_' % method
    db_name += str(date.year)
    if date.month < 10:
        db_name += '0%s' % str(date.month)
    else:
        db_name += str(date.month)
    if date.day < 10:
        db_name += '0%s' % str(date.day)
    else:
        db_name += str(date.day)
    return db_name

def save_rank_results(sorted_uids, identifyRange, method, date, window):
    if exist_rank_results(identifyRange, method, date, window):
        print 'range:%s method:%s date:%s window:%s rank results exist' % (identifyRange, method, date, window)
        return
    data = []
    rank = 1
    for uid in sorted_uids:
        if identifyRange == 'whole':
            item = WholeUserIdentification(rank=rank, userId=uid, identifyDate=date, identifyWindow=window, identifyMethod=method)
        elif identifyRange == 'burst':
            item = BurstUserIdentification(rank=rank, userId=uid, identifyDate=date, identifyWindow=window, identifyMethod=method)
        else:
            break
        db.session.add(item)
        rank += 1
    db.session.commit()

def exist_rank_results(identifyRange, method, date, window):
    if identifyRange == 'whole':
        items = db.session.query(WholeUserIdentification).filter_by(identifyMethod=method, identifyWindow=window, identifyDate=date)
    elif identifyRange == 'burst':
        items = db.session.query(BurstUserIdentification).filter_by(identifyMethod=method, identifyWindow=window, identifyDate=date)
    else:
        items = None
    if items and items.count():
        return True
    else:
        return False
