# -*- coding: utf-8 -*-

import os
import redis
from xapian_weibo.xapian_backend import XapianSearch


cron_start = '2013-9-1'
cron_end = '2013-10-2'
emotions_kv = {'happy': 1, 'angry': 2, 'sad': 3}
emotions_zh_kv = {'happy': '高兴', 'angry': '愤怒', 'sad': '悲伤'}
fields_value = {'culture', 'education', 'entertainment', 'fashion', 'finance', 'media', 'sports', 'technology', 'oversea', 'university', 'homeadmin', 'abroadadmin', 'homemedia', 'abroadmedia', 'folkorg', 'lawyer', 'politician', 'mediaworker', 'activer', 'grassroot', 'other'}
fields_id = {'culture': 0, 'education': 1, 'entertainment': 2, 'fashion': 3, 'finance': 4, 'media': 5, 'sports': 6, 'technology': 7, 'oversea': 8, 'university': 9, 'homeadmin': 10, 'abroadadmin': 11, 'homemedia': 12, 'abroadmedia': 13, 'folkorg': 14, 'lawyer': 15, 'politician': 16, 'mediaworker': 17, 'activer': 18, 'grassroot': 19, 'other': 20}
USER_DOMAIN = "user_domain" # user domain hash,
DOMAIN_LIST = ['culture', 'education', 'entertainment', 'fashion', 'finance', 'media', 'sports', 'technology', 'oversea', \
               'university', 'homeadmin', 'abroadadmin', 'homemedia', 'abroadmedia', 'folkorg', \
               'lawyer', 'politician', 'mediaworker', 'activer', 'grassroot', 'other']
DOMAIN_ZH_LIST = [u'文化', u'教育', u'娱乐', u'时尚', u'财经', u'媒体', u'体育', u'科技', u'境外', \
                  u'高校微博', u'境内机构', u'境外机构', u'媒体', u'境外媒体', u'民间组织', u'律师', \
                  u'政府官员', u'媒体人士', u'活跃人士', u'草根', u'其它']

LATEST_DATE = '20130904'

IS_PROD = 1

if IS_PROD:
    # XAPIAN_WEIBO_DATA_PATH = '/opt/xapian_weibo/data/20131210/'
    XAPIAN_USER_DATA_PATH = '/media/data/'
    # XAPIAN_DOMAIN_DATA_PATH = '/opt/xapian_weibo/data/20131130/'
    # MASTER_TIMELINE_STUB = '/home/ubuntu12/dev/data/stub/master_timeline_weibo_stub'
    LEVELDBPATH = '/media/data/leveldb'
    COBAR_HOST = '192.168.2.31'
    COBAR_PORT = 8066
    COBAR_USER = 'cobar'
    COBAR_DB = 'cobar_db_weibo'
    REDIS_HOST = '192.168.2.31'
    REDIS_PORT = 6379
    MYSQL_HOST = '192.168.2.31'
    MYSQL_USER = 'root'
    MYSQL_DB = 'weibo'
    ELEVATOR_HOST = '192.168.2.31'
    ELEVATOR_PORT = 4141
else:
    # XAPIAN_WEIBO_DATA_PATH = '/opt/xapian_weibo/data/20131210/'
    XAPIAN_USER_DATA_PATH = '/opt/xapian_weibo/data/20131221/'
    # XAPIAN_DOMAIN_DATA_PATH = '/opt/xapian_weibo/data/20131130/'
    # MASTER_TIMELINE_STUB = '/home/mirage/dev/data/stub/master_timeline_weibo_stub'
    LEVELDBPATH = '/home/mirage/leveldb'
    COBAR_HOST = '219.224.135.61'
    COBAR_PORT = 8066
    COBAR_USER = 'cobar'
    REDIS_HOST = '219.224.135.60'
    REDIS_PORT = 6379
    MYSQL_HOST = '219.224.135.60'
    MYSQL_USER = 'root'
    MYSQL_DB = 'weibo'

'''
READ_FROM_STUB = 1

if READ_FROM_STUB:
    xapian_search_weibo = XapianSearch(stub=MASTER_TIMELINE_STUB, schema_version=5)
else:
    xapian_search_weibo = XapianSearch(path=XAPIAN_WEIBO_DATA_PATH, name='master_timeline_weibo_csv', schema_version=5)
xapian_search_domain  = XapianSearch(path=XAPIAN_DOMAIN_DATA_PATH, name='master_timeline_domain', schema_version=4)
'''
xapian_search_user = XapianSearch(path=XAPIAN_USER_DATA_PATH, name='master_timeline_user', schema_version=1)
