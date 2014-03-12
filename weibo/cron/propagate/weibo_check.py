# -*- coding: utf-8 -*-

import time
from time_utils import ts2datetime
from calculate_single import calculate_single
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

        _update_single_status2Computing(mid, postDate, db_date)
        result  = calculate_single(mid)
        print result
        _update_single_status2Completed(mid, postDate, db_date)


if __name__ == '__main__':
    main()
