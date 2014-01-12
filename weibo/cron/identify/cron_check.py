# -*- coding: utf-8 -*-

from time_utils import ts2datetime
from area import pagerank_rank, degree_rank, make_network_graph
from utils import acquire_topic_name, acquire_topic_id, save_rank_results, save_gexf_results
from topicStatus import _topic_not_calc, _update_topic_status2Computing, _update_topic_status2Completed
import networkx as nx

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

        topic_id = int(topic_id)
        windowsize = int(windowsize)
        print date,topic_id,windowsize

        if not topic_id:
            gexf = ''
        else:
            gexf = make_network_graph(date, topic_id, topicname, windowsize)
        save_gexf_results(topicname, date, windowsize, gexf)
        #nx.write_gexf(gexf, "text.gexf")

        #topic_network() 
        _update_topic_status2Completed(topicname, start_ts, end_ts, db_date)
        
def topic_network():
    topics = _topic_not_calc()
    print 'catch it', topics
    if topics and len(topics):
        topic = topics[0]
        print topic

        start_ts = topic.start
        end_ts = topic.end
        db_date = topic.db_date
        topicname = topic.topic

        topic_id = acquire_topic_id(topicname, start_ts, end_ts)
        windowsize = (end_ts - start_ts) / Day
        date = ts2datetime(end_ts)

        if not topic_id:
            gexf = ''
        else:
            gexf = make_network_graph(date, topic_id, windowsize)
        output_file = open("gexf_network")
        nx.write_gexf(gexf, "text.gexf")


if __name__ == '__main__':
    main()
    #topic_network()
