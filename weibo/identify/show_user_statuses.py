# -*- coding: utf-8 -*-

from xapian_weibo.xapian_backend import XapianSearch

from time_utils import ts2datetime

def user_statuses(uid, page, page_num=10):
    query_dict = {'user': uid}
    start = (page-1) *  page_num
    end = start + page_num
    statuses_search = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_weibo', schema_version=2)
    statuses_count, get_statuses_results = statuses_search.search(query=query_dict, sort_by=['-timestamp'], field=['text', 'timestamp'], start_offset=start, max_offset=end)
    statuses = []
    if statuses_count:
        for status in get_statuses_results():
            text = status['text'].decode('utf-8')
            date = ts2datetime(status['timestamp'])
            statuses.append((text, date))
    return statuses
