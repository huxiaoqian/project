# -*- coding: utf-8 -*-


import leveldb
import os
import random
import sys

LEVELDBPATH = '/home/mirage/leveldb'
global_user_field_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_global_user_field'),
                                           block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
user_daily_field_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_user_daily_field'),
                                          block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

from xapian_weibo.xapian_backend import XapianSearch

user_search = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_user', schema_version=1)

query_dict = {
	'_id':{
		'$gt': 0,
		'$lt': 100000000000000000000
	}
}

count, get_results = user_search.search(query=query_dict)
print 'user count: ', count

fields_value = ['culture', 'education', 'entertainment', 'fashion', 'finance', 'media', 'sports', 'technology']
count = 0
for r in get_results():
	uid = r['_id']
	k = str(uid)
	try:
		updatedate = '20130430'
		fields = user_daily_field_bucket.Get(str(uid) + '_' + updatedate)
		if len(fields) == 2:
			v = ','.join[fields_value.index(fields[0]), fields_value.index(fields[1])]
		else:
			try:
				v = str(fields_value.index(fields[0]))
			except ValueError:
				continue
	except KeyError:
		v = str(random.randint(1,10))

	global_user_field_bucket.Put(k, v)
	if count % 10000 == 0:
		print count
	count += 1		