# -*- coding: utf-8 -*-

import time
from time_utils import ts2datetime
from calculate_single import calculate_single, calculate_part
from weiboStatus import _single_not_calc, _update_single_status2Computing, _update_single_status2Completed

TOPK = 1000
Minute = 60
Fifteenminutes = 15 * Minute
Hour = 3600
SixHour = Hour * 6
Day = Hour * 24
def datetime2ts(date):
    return time.mktime(time.strptime(date, '%Y-%m-%d %H:%M:%S'))

def main():
    weibos = _single_not_calc()
    if weibos and len(weibos):
    	weibo = weibos[0]

        mid = int(weibo.mid)
        postDate = str(weibo.postDate)
        db_date = weibo.db_date
        time_ts = datetime2ts(postDate)
        end_ts = time_ts + 24*3600

        result  = calculate_single(mid,time_ts,end_ts)#计算整个转发树结构
        print result
        idlist = [3617782506173763,3618043278635735,3618455003121922,3618481590955662,3618479728507301]
        #idlist表示以mid为顶点的子树结构中，包含的微博id（mid除外）
        #idlist必须要先构建整个转发树才能得到
        result  = calculate_part(mid,time_ts,end_ts,idlist)#计算整个子树结构
        print result
        _update_single_status2Completed(mid, postDate, db_date)


if __name__ == '__main__':
    main()