# -*- coding: utf-8 -*-


import time
from config import db
from model import WeiboStatus
from time_utils import datetime2ts

NOT_CALC_STATUS = -1
IN_CALC_STATUS = 0
COMPLETED_STATUS = 1

Minute = 60
Fifteenminutes = 15 * 60
Hour = 3600
SixHour = Hour * 6
Day = Hour * 24


def _single_not_calc(status=NOT_CALC_STATUS, module='propagate'):
    weibos_list = []
    weibos = db.session.query(WeiboStatus).filter(WeiboStatus.status==status, \
                                                  WeiboStatus.module==module).all()
    for weibo in weibos:
        weibos_list.append(weibo)

    return weibos_list


def _single_calculating(status=IN_CALC_STATUS, module='propagate'):
    weibos_list = []
    weibos = WeiboStatus.query.filter_by(status=status, module=module).all()
    for weibo in weibos:
        weibos_list.append(weibo.mid)

    return weibos_list


def _single_completed(status=COMPLETED_STATUS, module='propagate'):
    weibos_list = []
    weibos = WeiboStatus.query.filter_by(status=status, module=module).all()
    for weibo in weibos:
        weibos_list.append(weibo.mid)

    return weibos_list


def _update_single_status2Computing(mid, postDate, db_date, module='propagate'):
    item = WeiboStatus(module, IN_CALC_STATUS, mid, postDate, db_date)
    item_exist = db.session.query(WeiboStatus).filter(WeiboStatus.module==module, \
                                                      WeiboStatus.status==NOT_CALC_STATUS, \
                                                      WeiboStatus.mid==mid, \
                                                      WeiboStatus.postDate==postDate, \
                                                      WeiboStatus.db_date==db_date).first()
    if item_exist:
        db.session.delete(item_exist)
    db.session.add(item)
    db.session.commit()


def _update_single_status2Completed(mid, postDate, db_date, module='propagate'):
    item = WeiboStatus(module, COMPLETED_STATUS, mid, postDate, db_date)
    item_exist = db.session.query(WeiboStatus).filter(WeiboStatus.module==module, \
                                                      WeiboStatus.status==NOT_CALC_STATUS, \
                                                      WeiboStatus.mid==mid, \
                                                      WeiboStatus.postDate==postDate, \
                                                      WeiboStatus.db_date==db_date).first()
    if item_exist:
        db.session.delete(item_exist)
    db.session.add(item)
    db.session.commit()


def _drop_item(mid, postDate, db_date, status, module='propagate'):
    item_exist = db.session.query(WeiboStatus).filter(WeiboStatus.module==module, \
                                                      WeiboStatus.status==IN_CALC_STATUS, \
                                                      WeiboStatus.mid==mid, \
                                                      WeiboStatus.postDate==postDate, \
                                                      WeiboStatus.db_date==db_date).first()
    if item_exist:
        db.session.delete(item_exist)
    db.session.commit()


if __name__ == '__main__':
    _update_topic_status2Completed('高考', 1377964800, 1378051200)
    #_drop_item('日本', 1377964800, 1378310400, IN_CALC_STATUS)
    #_add_not_start_topic('开学')
