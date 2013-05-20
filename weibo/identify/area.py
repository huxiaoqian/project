# -*- coding: utf-8 -*-

import os
import sys
import select
import subprocess
import operator
import tempfile

import networkx as nx

from weibo.extensions import db
from time_utils import datetime2ts, window2time
from hadoop_utils import generate_job_id
from utils import save_rank_results, acquire_topic_name, is_in_black_list

from pagerank import pagerank

def degree_rank(top_n, date, topic_id, window_size):
    raise NotImplementedError

def pagerank_rank(top_n, date, topic_id, window_size):
    data = []

    tmp_file = prepare_data_for_pr(topic_id, date, window_size)

    if not tmp_file:
        return data

    input_tmp_path = tmp_file.name
    
    
    job_id = generate_job_id(datetime2ts(date), topic_id)
    iter_count = 1

    sorted_uids = pagerank(job_id, iter_count, input_tmp_path, top_n)
    
    data = save_rank_results(sorted_uids, 'area', 'pagerank', date, window_size, topic_id=topic_id)

    return data

def prepare_data_for_pr(topic_id, date, window_size):
    tmp_file = tempfile.NamedTemporaryFile(delete=False)
    
    topic = acquire_topic_name(topic_id)
    if not topic:
        return None
    end_time = datetime2ts(date)
    start_time = end_time - window2time(window_size)

    g = nx.DiGraph()

    #need repost index
    import random
    for i in range(15):
        repost_uid = random.randint(1, 20)
        source_uid = random.randint(1, 20)
        if is_in_black_list(repost_uid) or is_in_black_list(source_uid):
            continue
        g.add_edge(c1, c2)

    N = len(g.nodes())
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
