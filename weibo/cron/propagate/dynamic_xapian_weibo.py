# -*- coding: utf-8 -*-

import os
from config import DYNAMIC_XAPIAN_WEIBO_STUB_PATH, DYNAMIC_XAPIAN_WEIBO_STUB_FOLDER_PATH
from xapian_weibo.xapian_backend import XapianSearch

path = DYNAMIC_XAPIAN_WEIBO_STUB_PATH
folder_path = DYNAMIC_XAPIAN_WEIBO_STUB_FOLDER_PATH


def getXapianWeiboByDate(datestr):
    # datestr: 20130908    
    stub_file = path + datestr
    
    if os.path.exists(stub_file):
    	xapian_search_weibo = XapianSearch(stub=stub_file, include_remote=True)
    	return xapian_search_weibo
    else:
    	return None


def getXapianWeiboByDuration(datestr_list):
    stub_file_list = []

    for datestr in datestr_list:
        stub_file = path + datestr
	if os.path.exists(stub_file):
	    stub_file_list.append(stub_file)

    if len(stub_file_list):
        xapian_search_weibo = XapianSearch(stub=stub_file_list, include_remote=True)
        return xapian_search_weibo
    else:
    	return None


def target_whole_xapian_weibo():
    stub_files = os.listdir(folder_path)
    stub_file_list = []
    for f in stub_files:
        if os.path.isfile(os.path.join(folder_path, f)):
            stub_file_list.append(os.path.join(folder_path, f))

    if len(stub_file_list):
        xapian_search_weibo = XapianSearch(stub=stub_file_list, include_remote=True)
        return xapian_search_weibo
    else:
        return None
