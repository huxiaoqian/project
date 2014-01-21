# -*- coding: utf-8 -*-

from weibo.extensions import db
from weibo.model import WholeIdentification, ProfilePersonBasic
from weibo.global_config import xapian_search_user

from sqlalchemy import asc
from _mysql import _search_order_by_created_at, _search_by_domain
from time_utils import datetimestr2ts, ts2datetime, ts2HMS

thumbnail_user_fields = ['_id', 'followers_count', 'friends_count', 'statuses_count', \
                   			'active', 'important', 'description', 'verified', 'name', \
                   			'gender', 'profile_image_url', 'verified_type', 'location', \
                   			'description', 'created_at']

def thumbnail_user_info(uid):
		user = {}
		item = xapian_search_user.search_by_id(int(uid), fields=thumbnail_user_fields)
		if item:
				for f in thumbnail_user_fields:
						if f == 'verified':
								user[f] = u'是' if item[f] == True else u'否'
						elif f == 'created_at':
								try:
										user[f] = ts2HMS(item[f])
								except:
										user[f] = ''
						elif f == 'gender':
								user[f] = u'男' if item[f] == 'f' else u'女'
						else:
								user[f] = item[f]

		return user


def _hotest_users(limit=1000, date='2013-09-04'):
		users = []
		items = db.session.query(WholeIdentification)\
		                        .filter(WholeIdentification.identifyDate==date, \
			                              WholeIdentification.identifyWindow==1, \
			                              WholeIdentification.identifyMethod=='active')\
		                        .order_by(WholeIdentification.rank.asc())\
		                        .limit(limit)
		
		for user in items:
				thumbnail_user = thumbnail_user_info(user.userId)
				row_user = {'followers_count': user.followersCount, \
				            'active': user.activeCount, \
				            'important': user.importantCount}
				thumbnail_user.update(row_user)
				users.append(thumbnail_user)

		return users


def _newest_users(limit=1000, date='2013-09-04'):
		return _search_order_by_created_at(limit)


def _domain_users(domain, limit=1000, date='2013-09-04'):
		return _search_by_domain(domain, limit)
