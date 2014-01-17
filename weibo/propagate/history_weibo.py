# -*- coding: utf-8 -*-

import time
from weibo.extensions import db
from weibo.model import WeiboStatus as History

Minute = 60
Fifteenminutes = 15 * 60

def _all_history_weibo(status, module='propagate'):
    histories = History.query.filter_by(status=status, module=module).all()
    if histories:
        return 'success', histories
    else:
        return 'failed', None


def _search_history_weibo(mid, module='propagate'):
    histories = History.query.filter_by(topic=mid, status=1, module=module).all()
    if histories:
        return 'success', histories
    else:
        return 'failed', None


def _add_history_weibo(status, mid, postDate, db_date, module='propagate'):
    exist_item = History.query.filter_by(mid=mid, postDate=postDate, module=module).first()
    if exist_item:
        return 'failed', None
    else:
        item = History(module, status, mid, postDate, db_date)
        db.session.add(item)
        db.session.commit()
        return 'success', item
