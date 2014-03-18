# -*- coding: utf-8 -*-

from time_utils import ts2datetime
from autocalculate import calculate
from topicStatus import _topic_not_calc, _update_topic_status2Computing, _update_topic_status2Completed
import time
import datetime

TOPK = 1000
Minute = 60
Fifteenminutes = 15 * Minute
Hour = 3600
SixHour = Hour * 6
Day = Hour * 24

def main():
    topics = _topic_not_calc()
    if topics and len(topics):
    	topic = topics[0]

        start_ts = topic.start
        end_ts = topic.end
        db_date = topic.db_date
        topicname = topic.topic
        print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())), topicname.encode('utf-8'), 'start'
        _update_topic_status2Computing(topicname, start_ts, end_ts, db_date)
        result  = calculate(topicname,start_ts,end_ts)
        print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())), topicname.encode('utf-8'), result
        _update_topic_status2Completed(topicname, start_ts, end_ts, db_date)


if __name__ == '__main__':
    main()
