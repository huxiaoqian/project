# -*- coding: utf-8 -*-

import re
import time
from datetime import date
import datetime
from xapian_weibo.xapian_backend import XapianSearch

from weibo.model import HotStatus
from weibo.extensions import db
from xapian_config import xapian_search_weibo as s
def auto():
    
    #清除原数据
    for s in db.session.query(HotStatus):
        db.session.delete(s)
    db.session.commit()
    
    delta = datetime.timedelta(days = 90)
##    now = date.today()
    now = date.today()
    updatetime = date.today()
    now_year = int(now.year)
    now_month = int(now.month)
    now_day = int(now.day)
    end_ts1 = time.mktime(datetime.datetime(now_year, now_month, now_day).timetuple())

    bg = now - delta
    begin_ts1 = time.mktime(datetime.datetime(int(bg.year),int(bg.month),int(bg.day)).timetuple())
    
    
    #s = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_weibo', schema_version=2)
    count,get_results = s.search(query={'timestamp': {'$gt': begin_ts1, '$lt': end_ts1} }, sort_by=['-reposts_count'], max_offset = 10000,fields=['text','attitudes_count', 'source', 'timestamp','reposts_count','comments_count','user', '_id','retweeted_status'])
    print count
    i = 0
    for s in get_results():
	if i < 100:
	    s_id = s['_id']
	    text = s['text']
	    if s['source']:
                sourcePlatform = re.match('<.*?>(.*)<.*?>', s['source']).group(1)
            else:
                sourcePlatform = u'未知平台'
	    postDate = datetime.datetime.fromtimestamp(s['timestamp'])
	    if s['user']:
                uid = s['user']
            else:
                uid = None
            if s['retweeted_status']:
                retweetedMid = s['retweeted_status']
            else:
                retweetedMid = None
            if s['reposts_count']:
                repostsCount = s['reposts_count']
            else:
                repostsCount = 0
            if s['comments_count']:
                commentsCount = s['comments_count']
            else:
                commentsCount = 0
            if s['attitudes_count']:
                attitudesCount = s['attitudes_count']
            else:
                attitudesCount = 0
	    updateTime = updatetime
	    hot_status = HotStatus(id = s_id,text = text,sourcePlatform = sourcePlatform,postDate =postDate, uid = uid, retweetedMid = retweetedMid, repostsCount = repostsCount, commentsCount = commentsCount, attitudesCount = attitudesCount, updateTime = updateTime)
	    db.session.add(hot_status)
	    db.session.commit()
	    print i
	    i += 1
	else:
	    break

if __name__ == '__main__':auto()
