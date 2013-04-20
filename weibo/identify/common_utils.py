# -*- coding: utf-8 -*-

import operator
import time

import networkx as nx

import weibo.model

from weibo.extensions import db
from weibo.model import *

from time_utils import ts2datetime

def whole_followers_results(top_n, topic_id, w):
    window_date = ts2datetime(time.time())
    users = db.session.query(User).order_by(User.followersCount.desc()).limit(top_n)
    sorted_uids = []
    for user in users:
        sorted_uids.append(user.id)
    return sorted_uids

def burst_comments_results(top_n, topic_id, w):
    window_date = ts2datetime(time.time())
    users = db.session.query(User).order_by(User.followersCount.desc()).limit(top_n)
    sorted_uids = []
    for user in users:
        sorted_uids.append(user.id)
    return sorted_uids

def area_pagerank_results(top_n, topic_id, w):
    data = []
    window_date = ts2datetime(time.time())

    print 'start pagerank'
    g = nx.DiGraph()

    relations = db.session.query(RepostRelationship).filter_by(topicId=topic_id)
    if not relations:
        return data

    print 'total edegs %s' % relations.count()

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

    return sorted_uids

def common_results(topic_id, top_n, r, m, w):
    func_name = '%s_%s_results' % (r.lower(), m.lower())
    func = globals().get(func_name, None)
    return func(*(top_n, topic_id, w))
