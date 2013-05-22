# -*- coding: utf-8 -*-

import tempfile
import operator

import networkx as nx

from time_utils import datetime2ts, window2time
from hadoop_utils import generate_job_id
from utils import save_rank_results, acquire_topic_name, is_in_black_list, acquire_status_by_id
from config import PAGERANK_ITER_MAX

from xapian_weibo.xapian_backend import XapianSearch

from pagerank import pagerank

def degree_rank(top_n, date, topic_id, window_size):
    degree = prepare_data_for_degree(topic_id, date, window_size)
    sorted_degree = sorted(degree.iteritems(), key=operator.itemgetter(1), reverse=True)
    sorted_uids = []
    count = 0
    for uid, value in sorted_degree:
        if count > top_n:
            break
        sorted_uids.append(uid)
        count += 1

    data = save_rank_results(sorted_uids, 'area', 'degree', date, window_size, topic_id=topic_id)

    return data

def pagerank_rank(top_n, date, topic_id, window_size):
    data = []

    tmp_file = prepare_data_for_pr(topic_id, date, window_size)

    if not tmp_file:
        return data

    input_tmp_path = tmp_file.name
    
    
    job_id = generate_job_id(datetime2ts(date), topic_id)
    iter_count = PAGERANK_ITER_MAX

    sorted_uids = pagerank(job_id, iter_count, input_tmp_path, top_n)
    
    data = save_rank_results(sorted_uids, 'area', 'pagerank', date, window_size, topic_id=topic_id)

    return data

def prepare_data_for_degree(topic_id, date, window_size):
    topic = acquire_topic_name(topic_id)
    if not topic:
        return None
    end_time = datetime2ts(date)
    start_time = end_time - window2time(window_size)

    g = nx.DiGraph()

    #need repost index
    statuses_search = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_weibo', schema_version=2)
    query_dict = {'text': [topic], 'timestamp': {'$gt': start_time, '$lt': end_time}}

    count, get_statuses_results = statuses_search.search(query=query_dict, field=['text', 'user', 'retweeted_status'])
    print 'topic statuses count %s' % count

    for status in get_statuses_results():
        try:
            if status['retweeted_status']:
                repost_uid = status['user']
                rt_mid = status['retweeted_status']
                source_uid = acquire_status_by_id(rt_mid)['user']
                if is_in_black_list(repost_uid) or is_in_black_list(source_uid):
                    continue
                g.add_edge(repost_uid, source_uid)
        except KeyError:
            continue

    N = len(g.nodes())
    print 'topic network size %s' % N

    return g.degree()
 
def prepare_data_for_pr(topic_id, date, window_size):
    tmp_file = tempfile.NamedTemporaryFile(delete=False)
    
    topic = acquire_topic_name(topic_id)
    if not topic:
        return None
    end_time = datetime2ts(date)
    start_time = end_time - window2time(window_size)

    g = nx.DiGraph()

    #need repost index
    statuses_search = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_weibo', schema_version=2)
    query_dict = {'text': [topic], 'timestamp': {'$gt': start_time, '$lt': end_time}}

    count, get_statuses_results = statuses_search.search(query=query_dict, field=['text', 'user', 'retweeted_status'])
    print 'topic statuses count %s' % count

    for status in get_statuses_results():
        try:
            if status['retweeted_status']:
                repost_uid = status['user']
                rt_mid = status['retweeted_status']
                source_uid = acquire_status_by_id(rt_mid)['user']
                if is_in_black_list(repost_uid) or is_in_black_list(source_uid):
                    continue
                g.add_edge(repost_uid, source_uid)
        except KeyError:
            continue

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
