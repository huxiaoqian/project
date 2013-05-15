# -*- coding: utf-8 -*-

import operator

import networkx as nx

from weibo.model import RepostRelationship

from weibo.extensions import db
from time_utils import datetime2ts, window2time
from utils import save_rank_results

def simple_degreerank(top_n, post_date, topic_id, window_size):
    raise NotImplementedError

def simple_pagerank(top_n, post_date, topic_id, window_size):
    data = []

    print 'start pagerank'
    g = nx.DiGraph()

    relations = load_data(topic_id, post_date, window_size)
    if not relations:
        return data

    for relation in relations:
        source_uid = relation.sourceUid
        uid = relation.uid
        g.add_edge(uid, source_uid)

    total_vertex_size = len(g.nodes())
    degrees = nx.degree(g)
    #compute weekly connected components
    number = 0
    components = nx.weakly_connected_component_subgraphs(g)
    components_size = map(len, components)
    print '%s sub graph' % len(components)
    for index, component in enumerate(components):
        for node in component.node:
            g.node[node]['number'] = number
            g.node[node]['component'] = index
            number += 1
    page_ranks = [nx.pagerank(_g, weight='weight', max_iter=5000) for _g in components]
    new_page_rank = {}
    for page_rank in page_ranks:
        for name in page_rank:
            pr_value = page_rank[name]
            component_index = g.node[name]['component']
            pr_value *= components_size[component_index]*1.0 / total_vertex_size
            new_page_rank[name] = pr_value
    sorted_pr = sorted(new_page_rank.iteritems(), key=operator.itemgetter(1), reverse=True)

    if len(sorted_pr) > top_n:
        top_n += 1
        sorted_pr = sorted_pr[:top_n]

    sorted_uids = map(lambda x: x[0], sorted_pr)
    data = save_rank_results(sorted_uids, post_date=post_date, topic_id=topic_id, r='area', m='pagerank', w=window_size)
    return data

def load_data(topic_id, post_date, window_size):
    end_time = datetime2ts(post_date)
    start_time = end_time - window2time(window_size)
    relations = db.session.query(RepostRelationship).filter_by(topicId=topic_id)
    return relations
