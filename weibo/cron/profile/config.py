# -*- coding: utf-8 -*-

import os
from flask import Flask
from flask.ext.sqlalchemy import SQLAlchemy
from xapian_weibo.xapian_backend import XapianSearch

cron_start = '2013-9-1'
cron_end = '2013-9-6'
SIMULATE_BASE_DATE = '2013-12-22 19:00:00'

emotions_kv = {'happy': 1, 'angry': 2, 'sad': 3}
emotions_zh_kv = {'happy': '高兴', 'angry': '愤怒', 'sad': '悲伤'}
fields_value = ['culture', 'education', 'entertainment', 'fashion', 'finance', 'media', 'sports', 'technology', 'oversea']
fields_id = {'culture': 1, 'education': 2, 'entertainment': 3, 'fashion': 4, 'finance': 5, 'media': 6, 'sports': 7, 'technology': 8, 'oversea': 9}
labels = ['university', 'homeadmin', 'abroadadmin', 'homemedia', 'abroadmedia', 'folkorg', \
          'lawyer', 'politician', 'mediaworker', 'activer', 'grassroot', 'other']
zh_labels = ['高校微博', '境内机构', '境外机构', '媒体', '境外媒体', '民间组织', '律师', \
             '政府官员', '媒体人士', '活跃人士', '草根', '其它']
DOMAIN_LIST = fields_value + labels

IS_PROD = 1

if IS_PROD:
    #XAPIAN_WEIBO_DATA_PATH = '/opt/xapian_weibo/data/20131210/'
    XAPIAN_USER_DATA_PATH = '/media/data/'
    #XAPIAN_DOMAIN_DATA_PATH = '/opt/xapian_weibo/data/20131130/'
    MASTER_TIMELINE_STUB = '/home/ubuntu12/dev/data/stub/master_timeline_weibo_stub'
    LEVELDBPATH = '/media/data/leveldb'
    REDIS_HOST = '192.168.2.31'
    REDIS_PORT = 6379
    COBAR_HOST = '192.168.2.31'
    COBAR_PORT = 8066
    COBAR_USER = 'cobar'
    MYSQL_DB = 'weibo'
    MYSQL_HOST = '192.168.2.31'
    MYSQL_USER = 'root'
    SQLALCHEMY_DATABASE_URI = 'mysql+mysqldb://%s:@%s/%s?charset=utf8' % (MYSQL_USER, MYSQL_HOST, MYSQL_DB)
    DYNAMIC_XAPIAN_WEIBO_STUB_PATH = '/home/ubuntu12/dev/data/stub/master_timeline_weibo_'
else:
    XAPIAN_WEIBO_DATA_PATH = '/opt/xapian_weibo/data/20131210/'
    XAPIAN_USER_DATA_PATH = '/opt/xapian_weibo/data/20131221/'
    XAPIAN_DOMAIN_DATA_PATH = '/opt/xapian_weibo/data/20131130/'
    MASTER_TIMELINE_STUB = '/home/mirage/dev/data/stub/master_timeline_weibo_stub'
    LEVELDBPATH = '/home/mirage/leveldb'
    REDIS_HOST = '219.224.135.60'
    REDIS_PORT = 6379
    SQLALCHEMY_DATABASE_URI = 'mysql+mysqldb://root:@localhost/weibo?charset=utf8'
    DYNAMIC_XAPIAN_WEIBO_STUB_PATH = '/home/mirage/dev/data/stub/master_timeline_weibo_'


xapian_search_user = XapianSearch(path=XAPIAN_USER_DATA_PATH, name='master_timeline_user', schema_version=1)

# Create application
app = Flask('xxx')

# Create dummy secrey key so we can use sessions
app.config['SECRET_KEY'] = 'A0Zr98j/3yX R~XHH!jmN]LWX/,?RT'

# Create database
app.config['SQLALCHEMY_DATABASE_URI'] = SQLALCHEMY_DATABASE_URI
app.config['SQLALCHEMY_ECHO'] = False
db = SQLAlchemy(app)
