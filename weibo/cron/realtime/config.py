# -*- coding: utf-8 -*-

from flask import Flask
from flask.ext.sqlalchemy import SQLAlchemy
from xapian_weibo.xapian_backend import XapianSearch

REDIS_HOST = '192.168.2.31'
REDIS_PORT = 6379

COBAR_HOST = '192.168.2.31'
COBAR_PORT = 8066
COBAR_USER = 'cobar'

LEVELDBPATH = "/media/data/leveldb"

emotions_kv = {'happy': 1, 'angry': 2, 'sad': 3}

DOMAIN_LIST = ['culture', 'education', 'entertainment', 'fashion', 'finance', 'media', 'sports', \
               'technology', 'oversea', 'university', 'homeadmin', 'abroadadmin', 'homemedia', \
               'abroadmedia', 'folkorg', 'lawyer', 'politician', 'mediaworker', 'activer', \
               'grassroot', 'other']

XAPIAN_USER_DATA_PATH = '/media/data/' # '_master_timeline_user'
xapian_search_user = XapianSearch(path=XAPIAN_USER_DATA_PATH, name='master_timeline_user', schema_version=1)

DYNAMIC_XAPIAN_WEIBO_STUB_PATH = '/home/ubuntu12/dev/data/stub/master_timeline_weibo_'

MYSQL_USER = 'root'
MYSQL_HOST = '192.168.2.31'
MYSQL_DB = 'weibo'
SQLALCHEMY_DATABASE_URI = 'mysql+mysqldb://%s:@%s/%s?charset=utf8' % (MYSQL_USER, MYSQL_HOST, MYSQL_DB)

# Create application
app = Flask('xxx')

# Create dummy secrey key so we can use sessions
app.config['SECRET_KEY'] = 'A0Zr98j/3yX R~XHH!jmN]LWX/,?RT'

# Create database
app.config['SQLALCHEMY_DATABASE_URI'] = SQLALCHEMY_DATABASE_URI
app.config['SQLALCHEMY_ECHO'] = False
db = SQLAlchemy(app)
