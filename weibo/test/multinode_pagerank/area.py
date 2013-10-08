#coding:utf-8

import tempfile
import operator

import networkx as nx

from time_utils import datetime2ts, window2time, ts2datetime
from hadoop_utils import generate_job_id
from utils import is_in_trash_list, acquire_status_by_id, acquire_user_by_id, load_scws, cut, acquire_topic_name, acquire_topic_id
from config import PAGERANK_ITER_MAX

from flask import request

from xapian_weibo.xapian_backend import XapianSearch

from pagerank import pagerank

from gexf import Gexf
from lxml import etree

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

s = load_scws()

def pagerank_rank(top_n, date, topic_id, window_size):
    data = []

    tmp_file = prepare_data_for_pr(topic_id, date, window_size)

    if not tmp_file:
        return data

    input_tmp_path = tmp_file.name
    
    
    job_id = generate_job_id(datetime2ts(date), window_size, topic_id)
    iter_count = PAGERANK_ITER_MAX

    sorted_uids = pagerank(job_id, iter_count, input_tmp_path, top_n)

    print sorted_uids


def prepare_data_for_pr(topic_id, date, window_size):
    tmp_file = tempfile.NamedTemporaryFile(delete=False)

    topic = acquire_topic_name(topic_id)
    if not topic:
        return None

    g = make_network(topic, date, window_size)

    if not g:
        return None

    N = len(g.nodes())
    print 'topic network size %s' % N

    if not N:
        return None

    for node in g.nodes():
        outlinks = g.out_edges(nbunch=[node])
        outlinks = map(str, [n2 for n1, n2 in outlinks])
        if not outlinks:
            value = 'pr_results,%s,%s' % (1.0/N, N)
            tmp_file.write('%s\t%s\n' % (node, value))
        else:
            outlinks_str = ','.join(outlinks)
            value = 'pr_results,%s,%s,' % (1.0/N, N)
            value += outlinks_str
            tmp_file.write('%s\t%s\n' % (node, value))

    tmp_file.flush()
    return tmp_file

def make_network(topic, date, window_size, max_size=100000, ts=False):
    end_time = datetime2ts(date)
    start_time = end_time - window2time(window_size)

    g = nx.DiGraph()

    #need repost index
    topic = cut(s, topic.encode('utf-8'))
    statuses_search = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_weibo', schema_version=2)
    query_dict = {'text': topic, 'timestamp': {'$gt': start_time, '$lt': end_time}}

    if ts:
        count, get_statuses_results = statuses_search.search(query=query_dict, field=['text', 'user', 'timestamp', 'retweeted_status'], max_offset=max_size)
    else:
        count, get_statuses_results = statuses_search.search(query=query_dict, field=['text', 'user', 'retweeted_status'], max_offset=max_size)
    print 'topic statuses count %s' % count

    if ts:
        uid_ts = {}
        for status in get_statuses_results():
            try:
                if status['retweeted_status']:
                    repost_uid = status['user']
                    rt_mid = status['retweeted_status']
                    repost_ts = int(status['timestamp'])
                    source_status = acquire_status_by_id(rt_mid)
                    source_uid = source_status['user']
                    source_ts = int(source_status['timestamp'])
                    if is_in_trash_list(repost_uid) or is_in_trash_list(source_uid):
                        continue
                    if repost_uid not in uid_ts:
                        uid_ts[repost_uid] = repost_ts
                    else:
                        if uid_ts[repost_uid] > repost_ts:
                            uid_ts[repost_uid] = repost_ts
                    if source_uid not in uid_ts:
                        uid_ts[source_uid] = source_ts   
                    else:
                        if uid_ts[source_uid] > source_ts:
                            uid_ts[source_uid] = source_ts
                    g.add_edge(repost_uid, source_uid)
            except (TypeError, KeyError):
                continue
        return uid_ts, g
    else:
        for status in get_statuses_results():
            try:
                if status['retweeted_status']:
                    repost_uid = status['user']
                    rt_mid = status['retweeted_status']
                    source_uid = acquire_status_by_id(rt_mid)['user']
                    if is_in_trash_list(repost_uid) or is_in_trash_list(source_uid):
                        continue
                    g.add_edge(repost_uid, source_uid)
            except (TypeError, KeyError):
                continue
        return g

def main():
    top_n = 10
    date = ts2datetime(datetime2ts('2013-3-7'))
    topic_id = acquire_topic_id('春节')
    window_size = 1
    p = pagerank_rank(top_n, date, topic_id, window_size)


if __name__=='__main__': main()
