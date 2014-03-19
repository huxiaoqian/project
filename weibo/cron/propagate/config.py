# -*- coding: utf-8 -*-

import os
from flask import Flask
from flask.ext.sqlalchemy import SQLAlchemy
from xapian_weibo.xapian_backend import XapianSearch

cron_start = '2013-9-1'
cron_end = '2013-10-1' 

emotions_kv = {'happy': 1, 'angry': 2, 'sad': 3}
emotions_zh_kv = {'happy': '高兴', 'angry': '愤怒', 'sad': '悲伤'}
fields_value = {'culture', 'education', 'entertainment', 'fashion', 'finance', 'media', 'sports', 'technology'}
fields_id = {'culture': 1, 'education': 2, 'entertainment': 3, 'fashion': 4, 'finance': 5, 'media': 6, 'sports': 7, 'technology': 8}

IS_PROD = 1

if IS_PROD:
    XAPIAN_USER_DATA_PATH = '/media/data/'
    LEVELDBPATH = '/media/data/leveldb/'
    COBAR_HOST = '192.168.2.31'
    COBAR_PORT = 8066
    COBAR_USER = 'cobar'
    REDIS_HOST = '192.168.2.31'
    REDIS_PORT = 6379
    MYSQL_HOST = '192.168.2.31'
    MYSQL_USER = 'root'
    MYSQL_DB = 'weibo'
    ELEVATOR_HOST = '192.168.2.31'
    ELEVATOR_PORT = 4141
    SQLALCHEMY_DATABASE_URI = 'mysql+mysqldb://root:@localhost/weibo?charset=utf8'
    DYNAMIC_XAPIAN_WEIBO_STUB_PATH = '/home/ubuntu12/dev/data/stub/master_timeline_weibo_'
    DYNAMIC_XAPIAN_WEIBO_STUB_FOLDER_PATH = '/home/ubuntu12/dev/data/stub/'
else:
    XAPIAN_USER_DATA_PATH = '/opt/xapian_weibo/data/20131221/'
    LEVELDBPATH = '/home/mirage/leveldb'
    COBAR_HOST = '219.224.135.61'
    COBAR_PORT = 8066
    COBAR_USER = 'cobar'
    REDIS_HOST = '219.224.135.60'
    REDIS_PORT = 6379
    MYSQL_HOST = '219.224.135.60'
    MYSQL_USER = 'root'
    MYSQL_DB = 'weibo'
    SQLALCHEMY_DATABASE_URI = 'mysql+mysqldb://root:@localhost/weibo?charset=utf8'

xapian_search_user = XapianSearch(path=XAPIAN_USER_DATA_PATH, name='master_timeline_user', schema_version=1)

FOLLOWERS_MIN_SUPPORT = 500

REPOSTS_MIN_SUPPORT = 500

BURST_MIN_SUPPORT = 100

# Create application
app = Flask('xxx')

# Create dummy secrey key so we can use sessions
app.config['SECRET_KEY'] = 'A0Zr98j/3yX R~XHH!jmN]LWX/,?RT'

# Create database
app.config['SQLALCHEMY_DATABASE_URI'] = SQLALCHEMY_DATABASE_URI
app.config['SQLALCHEMY_ECHO'] = False
db = SQLAlchemy(app)
