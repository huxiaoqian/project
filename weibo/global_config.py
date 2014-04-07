# -*- coding: utf-8 -*-

import os
import redis
from xapian_weibo.xapian_backend import XapianSearch


emotions_kv = {'happy': 1, 'angry': 2, 'sad': 3}
emotions_zh_kv = {'happy': '高兴', 'angry': '愤怒', 'sad': '悲伤'}
fields_value = {'culture', 'education', 'entertainment', 'fashion', 'finance', 'media', 'sports', 'technology', 'oversea', 'university', 'homeadmin', 'abroadadmin', 'homemedia', 'abroadmedia', 'folkorg', 'lawyer', 'politician', 'mediaworker', 'activer', 'grassroot', 'other'}
fields_id = {'culture': 0, 'education': 1, 'entertainment': 2, 'fashion': 3, 'finance': 4, 'media': 5, 'sports': 6, 'technology': 7, 'oversea': 8, 'university': 9, 'homeadmin': 10, 'abroadadmin': 11, 'homemedia': 12, 'abroadmedia': 13, 'folkorg': 14, 'lawyer': 15, 'politician': 16, 'mediaworker': 17, 'activer': 18, 'grassroot': 19, 'other': 20}
USER_DOMAIN = "user_domain" # user domain hash,
DOMAIN_LIST = ['culture', 'education', 'entertainment', 'fashion', 'finance', 'media', 'sports', 'technology', 'oversea', \
               'university', 'homeadmin', 'abroadadmin', 'homemedia', 'abroadmedia', 'folkorg', \
               'lawyer', 'politician', 'mediaworker', 'activer', 'grassroot', 'other']
DOMAIN_ZH_LIST = [u'文化', u'教育', u'娱乐', u'时尚', u'财经', u'媒体', u'体育', u'科技', u'境外', \
                  u'高校微博', u'境内机构', u'境外机构', u'境内媒体', u'境外媒体', u'民间组织', u'律师', \
                  u'政府官员', u'媒体人士', u'活跃人士', u'草根', u'其它']

LATEST_DATE = '20130922'

IS_PROD = 1

if IS_PROD == 1:
    # 192.168.2.31
    XAPIAN_USER_DATA_PATH = '/media/data/'
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
    SSDB_PORT = 8888
    SSDB_HOST = '192.168.2.30'
    DYNAMIC_XAPIAN_WEIBO_STUB_PATH = '/home/ubuntu12/dev/data/stub/master_timeline_weibo_'
    DYNAMIC_XAPIAN_WEIBO_STUB_FOLDER_PATH = '/home/ubuntu12/dev/data/stub/'
    XAPIAN_RESTFUL_HOST = '192.168.2.31'
    XAPIAN_RESTFUL_PORT = 9004
elif IS_PROD == 0:
    # 192.168.2.30
    XAPIAN_USER_DATA_PATH = '/media/sdh/data/'
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
    SSDB_PORT = 8888
    SSDB_HOST = '192.168.2.30'
    DYNAMIC_XAPIAN_WEIBO_STUB_PATH = '/home/ubuntu12/dev/data/stub/master_timeline_weibo_'
    DYNAMIC_XAPIAN_WEIBO_STUB_FOLDER_PATH = '/home/ubuntu12/dev/data/stub/'
    XAPIAN_RESTFUL_HOST = '192.168.2.31'
    XAPIAN_RESTFUL_PORT = 9004
elif IS_PROD == -1:
    # 192.168.2.32
    XAPIAN_USER_DATA_PATH = '/home/ubuntu10/dev/data/'
    LEVELDBPATH = '/home/ubuntu10/dev/leveldb/'
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
    SSDB_PORT = 8888
    SSDB_HOST = '192.168.2.30'

xapian_search_user = XapianSearch(path=XAPIAN_USER_DATA_PATH, name='master_timeline_user', schema_version=1)
