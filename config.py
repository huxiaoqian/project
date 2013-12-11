# -*- coding: utf-8 -*-

from weibo.global_config import IS_PROD

# the debug toolbar is only enabled in debug mode
DEBUG = True

ADMINS = frozenset(['youremail@yourdomain.com'])
SECRET_KEY = 'SecretKeyForSessionSigning'

if IS_PROD:
    SQLALCHEMY_DATABASE_URI = 'mysql+mysqldb://root:@localhost/weibo?charset=utf8&passwd=Bh123456'
else:
    SQLALCHEMY_DATABASE_URI = 'mysql+mysqldb://root:@localhost/weibo?charset=utf8'
SQLALCHEMY_ECHO = False
DATABASE_CONNECT_OPTIONS = {}

THREADS_PER_PAGE = 8

CSRF_ENABLED = True
CSRF_SESSION_KEY= 'somethingimpossibletoguess'
