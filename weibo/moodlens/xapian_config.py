# -*- coding: utf-8 -*-

from xapian_weibo.xapian_backend import XapianSearch

xapian_search_user = XapianSearch(path='/opt/xapian_weibo/data/20131208/', name='master_timeline_user', schema_version=1)
#xapian_search_weibo = XapianSearch(path='/home/mirage/dev/data/20131207/', name='master_timeline_weibo', schema_version=2)
#xapian_search_sentiment = XapianSearch(stub=stub, name='master_timeline_sentiment', schema_version=3)
xapian_search_weibo = XapianSearch(path='/home/mirage/dev/data/20131210/', name='master_timeline_weibo_csv', schema_version=5)
xapian_search_domain  = XapianSearch(path='/opt/xapian_weibo/data/20131130/', name='master_timeline_domain', schema_version=4)

emotions_kv = {'happy': 1, 'angry': 2, 'sad': 3}
emotions_zh_kv = {'happy': '高兴', 'angry': '愤怒', 'sad': '悲伤'}
fields_value = {'culture', 'education', 'entertainment', 'fashion', 'finance', 'media', 'sports', 'technology'}
fields_id = {'culture':1, 'education':2, 'entertainment':3, 'fashion':4, 'finance':5, 'media':6, 'sports':7, 'technology':8}

LEVELDBPATH = '/home/mirage/leveldb'
