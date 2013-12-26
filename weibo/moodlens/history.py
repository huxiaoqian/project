# -*- coding: utf-8 -*-

import time
from weibo.extensions import db
from weibo.model import TopicStatus as History


def _all_history(status):
	histories = History.query.filter_by(status=status).all()
	if histories:
		return 'success', histories
	else:
		return 'failed', None


def _search_history(topic):
	histories = History.query.filter_by(topic=topic, status=1).all()
	if histories:
		return 'success', histories
	else:
		return 'failed', None


def _add_history(module, status, topic, start, end, range, db_date):
	exist_item = History.query.filter_by(topic=topic, start=start, end=end).first()
	if exist_item:
		return 'failed', None
	else:
		item = History('sentiment', status, topic, start, end, range, db_date)
		db.session.add(item)
		db.session.commit()
		return 'success', item