# -*- coding: utf-8 -*-

from xapian_weibo.xapian_backend import XapianSearch
'''
search_weibo = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_weibo', schema_version=2)
print search_weibo.search(query={'_id': {'gt': 0, 'lt': 30000000000000000000000000}}, count_only=True)

search_user = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_user', schema_version=1)
print search_user.search(query={'_id': {'gt': 0, 'lt': 300000000000000}}, count_only=True)
'''
begin_ts = 0
end_ts = 12349873894898
query_dict = {
    'timestamp': {'$gt': begin_ts, '$lt': end_ts},
}
xapian_search_sentiment = XapianSearch(path='/opt/xapian_weibo/data/20130807', name='master_timeline_sentiment', schema_version=3)
print xapian_search_sentiment.search(query=query_dict, count_only=True)
