# -*- coding: utf-8 -*-

import os
from xapian_weibo.xapian_backend import XapianSearch

from time_utils import ts2datetime
path = '/home/mirage/dev/data/stub/master_timeline_weibo_'

def getXapianWeiboByDate(datestr):
    # datestr: 20130908
    
    stub_file = path + datestr
    
    if os.path.exists(stub_file):
            xapian_search_weibo = XapianSearch(stub=stub_file, include_remote=True)
            return xapian_search_weibo
    else:
            return None

def user_statuses(uid, page, time_ts, page_num=10):
    
    query_dict = {'user': uid}
    start = (page-1) *  page_num
    end = start + page_num

    date = ts2datetime(time_ts)
    datestr = date.replace('-', '')
    xapian_search_weibo = getXapianWeiboByDate(datestr)
    
    statuses_count, get_statuses_results = xapian_search_weibo.search(query=query_dict, sort_by=['-timestamp'], field=['text', 'timestamp'], start_offset=start, max_offset=end)
    statuses = []

    if statuses_count:
        for status in get_statuses_results():
            text = status['text']
            date = ts2datetime(status['timestamp'])
            statuses.append((text, date))
    return statuses
