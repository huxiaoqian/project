# -*- coding: utf-8 -*-


import leveldb
import json
import os


LEVELDBPATH = '/home/mirage/leveldb'

weibo_repost_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_global_weibo_repost_20131004'),
                                      block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

from xapian_weibo.xapian_backend import XapianSearch
statuses_search = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_weibo', schema_version=2)


def search_by_uid(uid):
	query_dict = {'user': int(uid)}
	count, get_results = statuses_search.search(query=query_dict, fields=['_id', 'retweeted_status', 'timestamp'])
	for r in get_results():
		if r['retweeted_status'] != None:
			mid = r['_id']
			ts = r['timestamp']
			key_str = str(mid)
			uid_ = None
			uid__ = None
			try:
				mid_, uid_ = weibo_repost_bucket.Get(key_str).split('_')
				try:
					mid__, uid__ = weibo_repost_bucket.Get(mid_).split('_')
				except KeyError:
					pass
			except KeyError:
				continue
			if uid__ and uid_:
				print uid, uid_, uid__


if __name__ == '__main__':
	search_by_uid(1197161814)
