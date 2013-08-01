# -*- coding: utf-8 -*-

import tempfile
import operator

import networkx as nx

from weibo.extensions import db
from weibo.model import AreaUserIdentification

from time_utils import datetime2ts, window2time
from hadoop_utils import generate_job_id
from utils import save_rank_results, acquire_topic_name, is_in_trash_list, acquire_status_by_id, acquire_user_by_id, load_scws, cut
from config import PAGERANK_ITER_MAX

from xapian_weibo.xapian_backend import XapianSearch

from pagerank import pagerank

from gexf import Gexf
from lxml import etree

s = load_scws()

def degree_rank(top_n, date, topic_id, window_size):
    data = []
    degree = prepare_data_for_degree(topic_id, date, window_size)

    if not degree:
        return data

    sorted_degree = sorted(degree.iteritems(), key=operator.itemgetter(1), reverse=True)
    sorted_uids = []
    count = 0
    for uid, value in sorted_degree:
        if count >= top_n:
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
    
    
    job_id = generate_job_id(datetime2ts(date), window_size, topic_id)
    iter_count = PAGERANK_ITER_MAX

    sorted_uids = pagerank(job_id, iter_count, input_tmp_path, top_n)
    
    data = save_rank_results(sorted_uids, 'area', 'pagerank', date, window_size, topic_id=topic_id)

    return data

def prepare_data_for_degree(topic_id, date, window_size):
    g = make_network(topic_id, date, window_size)

    if not g:
        return None

    N = len(g.nodes())
    print 'topic network size %s' % N

    if not N:
        return None

    return g.degree()

def prepare_data_for_pr(topic_id, date, window_size):
    tmp_file = tempfile.NamedTemporaryFile(delete=False)
    
    g = make_network(topic_id, date, window_size)

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

def make_network_graph(current_date, topic_id, window_size, key_user_labeled=True):
    date = current_date

    if key_user_labeled:
        key_users = read_key_users(current_date, window_size, topic_id, top_n=10)
    else:
        key_users = []
              
    uid_ts, G = make_network(topic_id, date, window_size, ts=True)

    N = len(G.nodes())

    if not N:
        return ''

    node_degree = nx.degree(G)

    G = cut_network(G, node_degree)
    
    gexf = Gexf("Yang Han", "Topic Network")

    node_id = {}
    graph = gexf.addGraph("directed", "static", "demp graph")
    graph.addNodeAttribute('name', type='string', force_id='name')
    graph.addNodeAttribute('location', type='string', force_id='location')
    graph.addNodeAttribute('timestamp', type='int', force_id='timestamp')

    pos = nx.spring_layout(G)

    node_counter = 0
    edge_counter = 0

    for node in G.nodes():
        x, y = pos[node]
        degree = node_degree[node]
        if node not in node_id:
            node_id[node] = node_counter
            node_counter += 1
        uid = node
        if uid in key_users:
            _node = graph.addNode(node_id[node], str(node), x=str(x), y=str(y), z='0', r='255', g='51', b='51', size=str(degree))
        else:
            _node = graph.addNode(node_id[node], str(node), x=str(x), y=str(y), z='0', r='0', g='204', b='204', size=str(degree))
        user_info = acquire_user_by_id('area', uid)
        if user_info:
            _node.addAttribute('name', user_info['name'].decode('utf-8'))
            _node.addAttribute('location', user_info['location'].decode('utf-8'))
        else:
            _node.addAttribute('name', 'Unknown')
            _node.addAttribute('location', 'Unknown')
        _node.addAttribute('timestamp', str(uid_ts[uid]))

    for edge in G.edges():
        start, end = edge
        start_id = node_id[start]
        end_id = node_id[end]
        graph.addEdge(str(edge_counter), str(start_id), str(end_id))
        edge_counter += 1

    return etree.tostring(gexf.getXML(), pretty_print=True, encoding='utf-8', xml_declaration=True)

def read_key_users(date, window, topic_id, top_n=10):
    items = db.session.query(AreaUserIdentification).filter_by(topicId=topic_id, identifyWindow=window, identifyDate=date).order_by(AreaUserIdentification.rank.asc()).limit(top_n)
    users = []
    if items.count():
        for item in items:
            uid = item.userId
            users.append(uid)
    return users
    
def cut_network(g, node_degree):
    degree_threshold = 2
    for node in g.nodes():
        degree = node_degree[node]
        if degree < degree_threshold:
            g.remove_node(node)
    return g

def make_network(topic_id, date, window_size, max_size=100000, ts=False):
    topic = acquire_topic_name(topic_id)
    if not topic:
        return None
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
