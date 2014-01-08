# -*- coding: utf-8 -*-

from time_utils import ts2datetime
from area import pagerank_rank, degree_rank
from utils import acquire_topic_name, acquire_topic_id, save_rank_results
from topicStatus import _topic_not_calc, _update_topic_status2Computing, _update_topic_status2Completed

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

        topic_id = acquire_topic_id(topicname, start_ts, end_ts)
        windowsize = (end_ts - start_ts) / Day
        date = ts2datetime(end_ts)

        if windowsize > 7:
            degree_rank(TOPK, date, topic_id, windowsize)
        else:
            pagerank_rank(TOPK, date, topic_id, windowsize)

        _update_topic_status2Completed(topicname, start_ts, end_ts, db_date)


if __name__ == '__main__':
    main()
