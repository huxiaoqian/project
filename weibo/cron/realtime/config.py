# -*- coding: utf-8 -*-

from xapian_weibo.xapian_backend import XapianSearch

REDIS_HOST = '192.168.2.11'
REDIS_PORT = 6379

GLOBAL_ACTIVE_COUNT = "global_active_%s" # date as '20131227',
GLOBAL_IMPORTANT_COUNT = "global_important_%s" # date as '20131227',

LEVELDBPATH = "/media/data/leveldb"

DOMAIN_LIST = ['culture', 'education', 'entertainment', 'fashion', 'finance', 'media', 'sports', 'technology', 'oversea', \
               'university', 'homeadmin', 'abroadadmin', 'homemedia', 'abroadmedia', 'folkorg', \
               'lawyer', 'politician', 'mediaworker', 'activer', 'grassroot', 'other']

XAPIAN_USER_DATA_PATH = '/opt/xapian_weibo/data/20131221/'
xapian_search_user = XapianSearch(path=XAPIAN_USER_DATA_PATH, name='master_timeline_user', schema_version=1)