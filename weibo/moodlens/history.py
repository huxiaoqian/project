# -*- coding: utf-8 -*-

import time
from weibo.extensions import db
from weibo.model import TopicStatus as History

#对topic操作的历史进行遍历、查询已经添加的历史，添加历史
def _all_history(status, module='sentiment'):
	histories = History.query.filter_by(status=status, module=module).all()
	if histories:
		return 'success', histories
	else:
		return 'failed', None


def _search_history(topic, module='sentiment'):
	histories = History.query.filter_by(topic=topic, status=1, module=module).all()
	if histories:
		return 'success', histories
	else:
		return 'failed', None


def _add_history(status, topic, start, end, db_date, range, module='sentiment'):
	exist_item = History.query.filter_by(topic=topic, start=start, end=end, module=module).first()
	if exist_item:
		return 'failed', None
	else:
		item = History(module, status, topic, start, end, range, db_date)
		db.session.add(item)
		db.session.commit()
		return 'success', item
