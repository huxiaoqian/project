# -*- coding: utf-8 -*-

import os
from xapian_weibo.xapian_backend import XapianSearch

path = '/home/mirage/dev/data/stub/master_timeline_weibo_'


def getXapianWeiboByDate(datestr):
    # datestr: 20130908
    
    stub_file = path + datestr
    
    if os.path.exists(stub_file):
    	xapian_search_weibo = XapianSearch(stub=stub_file, include_remote=True)
    	return xapian_search_weibo
    else:
    	return None
