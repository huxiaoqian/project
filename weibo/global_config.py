# -*- coding: utf-8 -*-

import os
from xapian_weibo.xapian_backend import XapianSearch


cron_start = '2013-9-1'
cron_end = '2013-10-2'
emotions_kv = {'happy': 1, 'angry': 2, 'sad': 3}
emotions_zh_kv = {'happy': '高兴', 'angry': '愤怒', 'sad': '悲伤'}
fields_value = {'culture', 'education', 'entertainment', 'fashion', 'finance', 'media', 'sports', 'technology'}
fields_id = {'culture': 1, 'education': 2, 'entertainment': 3, 'fashion': 4, 'finance': 5, 'media': 6, 'sports': 7, 'technology': 8}

IS_PROD = 0

if IS_PROD:
    XAPIAN_WEIBO_DATA_PATH = '/opt/xapian_weibo/data/20131210/'
    XAPIAN_USER_DATA_PATH = '/opt/xapian_weibo/data/20131208/'
    XAPIAN_DOMAIN_DATA_PATH = '/opt/xapian_weibo/data/20131130/'
    MASTER_TIMELINE_STUB = '/home/ubuntu12/dev/data/stub/master_timeline_weibo_stub'
    LEVELDBPATH = '/home/mirage/leveldb'
else:
    XAPIAN_WEIBO_DATA_PATH = '/opt/xapian_weibo/data/20131210/'
    XAPIAN_USER_DATA_PATH = '/opt/xapian_weibo/data/20131208/'
    XAPIAN_DOMAIN_DATA_PATH = '/opt/xapian_weibo/data/20131130/'
    MASTER_TIMELINE_STUB = '/home/mirage/dev/data/stub/master_timeline_weibo_stub'
    LEVELDBPATH = '/home/mirage/leveldb'

READ_FROM_STUB = 1

if READ_FROM_STUB:
    xapian_search_weibo = XapianSearch(stub=MASTER_TIMELINE_STUB, schema_version=5)
else:
	xapian_search_weibo = XapianSearch(path=XAPIAN_WEIBO_DATA_PATH, name='master_timeline_weibo_csv', schema_version=5)

xapian_search_domain  = XapianSearch(path=XAPIAN_DOMAIN_DATA_PATH, name='master_timeline_domain', schema_version=4)
xapian_search_user = XapianSearch(path=XAPIAN_USER_DATA_PATH, name='master_timeline_user', schema_version=1)
