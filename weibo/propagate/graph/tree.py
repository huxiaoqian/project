# -*- coding: utf-8 -*-

import math
import re
import csv
import random
import operator
import codecs
from xapian_weibo.xapian_backend import XapianSearch
import sys
import os
import time

import base62
import buchheim

from lxml import etree

from gexf import Gexf
from gen import Tree
try:
    xapian_search_user = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_user', schema_version=1)
    s = XapianSearch(path='/opt/xapian_weibo/data/',name='master_timeline_weibo',schema_version=2)
except:
    print 'sth. wrong with xapian, please check propagate/graph/tree.py'
class Count:
    def __init__(self, count=0):
        self.count = count

def reposts2tree(source_weibo, reposts):
    # root
    tree_nodes = []
    count, get_results = xapian_search_user.search(query={'_id': source_weibo['user']}, fields=['profile_image_url', 'name' , 'location'])#查找原创微博的用户信息
    for result in get_results():
    
        node = result['name'].decode('utf-8')
        location = result['location'].decode('utf-8') 
        datetime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(source_weibo['timestamp'])) 
        img_url = result['profile_image_url'].decode('utf-8') 
        weibo_url = base62.weiboinfo2url(source_weibo['user'], str(source_weibo['_id']))
        tree_nodes.append(Tree(node, location, datetime, source_weibo['_id'], img_url, weibo_url))

    # sort reposts
    reposts = sorted(reposts, key=lambda x: x['_id'])

    # genarate tree
    for repost in reposts:
        count, get_results = xapian_search_user.search(query={'_id': repost['user']}, fields=['profile_image_url', 'name' , 'location'])#查找转发微博的用户信息
        for result in get_results():
            node = result['name'].decode('utf-8') 
            img_url = result['profile_image_url'].decode('utf-8')
            location = result['location'].decode('utf-8')
            datetime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(repost['timestamp'])) 
            weibo_url = base62.weiboinfo2url(repost['user'], str(repost['_id']))
            uid = repost['user']
            tree_nodes.append(Tree(node, location, datetime, repost['_id'], img_url, weibo_url, uid))

            repost_users = re.findall(u'/@([a-zA-Z-_\u0391-\uFFE5]+)', repost['text'].decode('utf-8'))
            parent_idx = 0
            while parent_idx < len(repost_users):
                flag = False
                for node in tree_nodes[-2::-1]:
                
                    if node.node == repost_users[parent_idx]:
                        node.append_child(tree_nodes[-1])
                        flag = True
                        break

                if flag:
                    break
                parent_idx += 1
            else:
                tree_nodes[0].append_child(tree_nodes[-1])
            

    return tree_nodes

def add_node_and_edge(drawtree, graph, ct, parent=None, max_width=0):
    length = len(drawtree.children)
    size = math.log((math.pow(length, 0.3) + math.sqrt(4)), 4)
    uid = drawtree.tree.uid

    b, r, g = '255', '0', '0'

    scale_y = max_width / 200 + 1
    node = graph.addNode(drawtree.tree.wid, drawtree.tree.node,
                         b=b, r=r, g=g, x=str(drawtree.x), y=str(drawtree.y * scale_y * 10), z='0.0',
                         size=str(size))

    node.addAttribute('img_url', drawtree.tree.img_url)
    node.addAttribute('name', drawtree.tree.node)
    node.addAttribute('location', drawtree.tree.location)
    node.addAttribute('datetime', drawtree.tree.datetime)
    node.addAttribute('repost_num', str(length))
    node.addAttribute('weibo_url', drawtree.tree.weibo_url)

    if parent is not None:
        ct.count += 1
        graph.addEdge(ct.count, str(drawtree.tree.wid), str(parent.tree.wid))

    for child in drawtree.children:
        add_node_and_edge(child, graph, ct, drawtree, max_width)

def tree2graph(tree_nodes):
    dt, max_width = buchheim.buchheim(tree_nodes[0])

    gexf = Gexf('MOON_CLJ', 'simple')
    graph = gexf.addGraph('directed', 'static', 'weibo graph')
    graph.addNodeAttribute('img_url', type='URI', force_id='img_url')
    graph.addNodeAttribute('name', type='string', force_id='name')
    graph.addNodeAttribute('location', type='string', force_id='location')
    graph.addNodeAttribute('datetime', type='string', force_id='datetime')
    graph.addNodeAttribute('repost_num', type='integer', force_id='repost_num')
    graph.addNodeAttribute('weibo_url', type='URI', force_id='weibo_url')

    add_node_and_edge(dt, graph, Count(), max_width=max_width)

    return etree.tostring(gexf.getXML(), pretty_print=True, encoding='utf-8', xml_declaration=True)

def tree_main(mid):
    mids = [mid]
    
    for mid in mids:
        users = set()
        number,source_weibo = s.search(query={'retweeted_status': mid})#读取以mid为原创微博的转发微博
        assert source_weibo, 'Source Weibo exist ???'
        repost_ids = []
        for sw in source_weibo():
            repost_ids.append(sw['_id']) 
        if not number:
            continue
        count = 0
        reposts = []#存储转发微博的信息
        for sid in repost_ids:
            if count % 10 == 0:
                print '%s tweets loaded...' % count
            n,ws = s.search(query={'_id': sid})#查找转发微博的信息
            for w in ws():
                try:
                    users.add(w['user'])
                except KeyError:
                    continue
                reposts.append(w)
                count += 1

        n,m_weibo = s.search(query={'_id': mid})#查找原创微博的信息
        for m_w in m_weibo():
            source_weibo = m_w

        tree = reposts2tree(source_weibo, reposts)
        graph = tree2graph(tree)

        with open('./weibo/static/gexf/tree.gexf', 'w') as gf:
            gf.write(graph)
