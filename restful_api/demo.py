# -*- coding: utf-8 -*-

import requests as pyrequests
from config import XAPIAN_RESTFUL_HOST, XAPIAN_RESTFUL_PORT

mid = '3617892379961181'
search_start_ts = '1377964800'
search_end_ts = '1379692800'

# 正常在全库根据mid搜索有无该条微博，有则返回字符串'true', 无则返回'false'
resp = pyrequests.get('http://%s:%s/status_exist/%s' % (XAPIAN_RESTFUL_HOST, XAPIAN_RESTFUL_PORT, mid))
if resp.text.strip() == 'true' and resp.status_code==200:
	  print u'有该条微博'
else:
	  print u'无该条微博'

# 指定时间段时根据mid搜索有无该条微博，有则返回字符串'true', 无则返回'false'
resp = pyrequests.get('http://%s:%s/status_exist/%s/%s/%s' % (XAPIAN_RESTFUL_HOST, XAPIAN_RESTFUL_PORT, mid, search_start_ts, search_end_ts))
if resp.text.strip() == 'true' and resp.status_code==200:
	  print u'有该条微博'
else:
	  print u'无该条微博'
