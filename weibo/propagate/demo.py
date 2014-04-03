# -*- coding: utf-8 -*-

import json
import requests as pyrequests
from weibo.global_config import XAPIAN_RESTFUL_HOST, XAPIAN_RESTFUL_PORT

api_host = 'http://%s:%s/status_exist/{mid}' % (XAPIAN_RESTFUL_HOST, XAPIAN_RESTFUL_PORT)
api_host_ts = 'http://%s:%s/status_exist/{mid}/{start_ts}/{end_ts}' % (XAPIAN_RESTFUL_HOST, XAPIAN_RESTFUL_PORT)


def search_status_by_mid(mid):
    # 正常在全库根据mid搜索有无该条微博，有则返回字符串'true', 无则返回'false'
    resp = pyrequests.get(api_host.format(mid=mid))
    resp_json = resp.json()
    if resp.status_code==200 and resp_json['status']=='true':
        print resp_json['status'], resp_json['data']
        return resp_json['status'], resp_json['data']
    else:
        return 'false', ''


def search_status_by_mid_ts(mid, search_start_ts, search_end_ts):
    # 指定时间段时根据mid搜索有无该条微博，有则返回字符串'true', 无则返回'false'
    resp = pyrequests.get(api_host_ts.format(mid=mid, start_ts=search_start_ts, end_ts=search_end_ts))
    resp_json = resp.json()
    if resp.status_code==200 and resp_json['status']=='true':
        print resp_json['status'], resp_json['data']
        return resp_json['status'], resp_json['data']
    else:
        return 'false', ''


if __name__ == '__main__':
    mid = '3617892379961181'
    search_start_ts = '1377964800'
    search_end_ts = '1379692800'
	  
    search_status_by_mid(mid)
    search_status_by_mid_ts(mid, search_start_ts, search_end_ts)
