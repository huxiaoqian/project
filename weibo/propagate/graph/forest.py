#-*- coding: utf-8 -*-

import os
import re
import time
import codecs
import random
import math
import sys

from collections import defaultdict

from gexf import Gexf
from lxml import etree
from xapian_weibo.xapian_backend import XapianSearch

sys.path.append('./weibo/propagate/')
from autocalculate import calculate
from calculate_single import calculate_single,get_user
from calculatetopic import calculate_topic

MAX_COUNT = 15000
START_DATE = '2012-9-1'
#END_DATE = '2012-10-30'
FLOAT_FORMAT = '%.2f'
SEG = 2

xapian_search_user = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_user', schema_version=1)
s = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_weibo', schema_version=2)

def date2ts(date):
    return int(time.mktime(time.strptime(date, '%Y-%m-%d')))

def unix2local(ts):
    return time.strftime('%Y-%m-%d %H:00', time.localtime(ts))

def load_data(keyword,beg_time,end_time):
    dataset = []
    fields_list = ['text', 'timestamp','reposts_count','comments_count','user', 'terms', '_id','retweeted_status','bmiddle_pic','geo','source','attitudes_count']
    number, get_results = s.search(query={'text': [u'%s'%keyword], 'timestamp': {'$gt': beg_time, '$lt': end_time}}, sort_by=['timestamp'], fields=fields_list)
    topic_info = calculate(get_results())
    blog_rel_list = topic_info['topic_rel_blog'][:5]

    ts = []
    for status in blog_rel_list:
        print status['status']['_id']
        ts.append(int(time.mktime(time.strptime(str(status['status']['created_at']), '%Y-%m-%d %H:%M:%S'))))
        number,source_weibo = s.search(query={'retweeted_status': status['status']['_id']})#查找热门微博的转发微博
        #repost_ids = []
        if not number:
            continue

        count = 0
        reposts = []#存储转发微博的信息
        for sw in source_weibo():
            if count % 10 == 0:
                print '%s tweets loaded...' % count
            number,get_users = xapian_search_user.search(query={'_id': sw['user']}, fields=['name'])
            for user in get_users():
                line = unicode(user['name'], 'utf-8') + '\t'+ unicode(sw['text'], 'utf-8') + '\t' + unicode(status['user']['name'], 'utf-8') +'\t'+ str(int(sw['timestamp']))
                reposts.append(line)
            count += 1
        dataset.append(reposts)
            #repost_ids.append(sw['_id'])         
        #number,source_users = xapian_search_user.search(query={'_id': status['status']['user']['id']}, fields=['name'])
        #for source_user in source_users():
            #su = source_user['name']
##        count = 0
##        reposts = []#存储转发微博的信息
##        for sid in repost_ids:
##            if count % 10 == 0:
##                print '%s tweets loaded...' % count
##            n,ws = s.search(query={'_id': sid})#查找转发微博的信息
##            for w in ws():
##                number,get_users = xapian_search_user.search(query={'_id': w['user']}, fields=['name'])
##                for user in get_users():
##                    line = unicode(user['name'], 'utf-8') + '\t'+ unicode(w['text'], 'utf-8') + '\t' + unicode(status['status']['user']['name'], 'utf-8') +'\t'+ str(int(w['timestamp']))
##                    reposts.append(line)
##                count += 1
##        dataset.append(reposts)
    return min(ts), dataset  

def random_color(colors):
    r  = str(int(random.random()*255))
    g  = str(int(random.random()*255))
    b  = str(int(random.random()*255))
    while (r, g, b) in colors:
        r  = str(int(random.random()*255))
        g  = str(int(random.random()*255))
        b  = str(int(random.random()*255))
    colors.append((r, g, b))
    return r, g, b

def ts2x(ts, mode='4hour'):
    if mode == 'log':
        return int(math.log(math.fabs(ts)+1, 10))
    elif mode == '4hour':
        return int(float(ts)/14400)
    else:
        return int(float(ts)/3600)

def x2date(x, start_ts, mode='4hour'):
    if mode == '4hour':
        start_date = unix2local(start_ts+x*14400)
        return start_date
    elif mode == 'log':
        start_date = unix2local(start_ts+10**(x-1))
        return start_date
    else:
        start_date = unix2local(start_ts+x*3600)
        return start_date

def build_x_chart(graph, x_count, counter, y, start_ts):
    max_count = max(x_count.values())
    x_count = sorted(x_count.iteritems(), key=lambda (k, v): k)
    previous = None
    for x, count in x_count:
        counter += 1
        graph.addNode(counter, str('%s' % x2date(x, start_ts)), x=str(x), y=str(y*(1+0.5*(float(count)/max_count))), z='0', r='0', g='0', b='0', size='5')
        if previous:
            graph.addEdge(str(previous+counter), str(previous), str(counter))
        previous = counter
    return counter

def build_y_chart(graph, y_count, y_name, counter, x):
    y_count = sorted(y_count.iteritems(), key=lambda (k, v): k)
    for y, count in y_count:
        counter += 1
        graph.addNode(counter, '%s %s time(s)' % (y_name[y], count), x=str(x*(1+0.1*float(count))), y=str(y), z='0', r='0', g='0', b='0', size='0.1')
        if count > 1:
            counter += 1
            graph.addNode(counter, '0', x=str(x*(1+0.1*float(1))), y=str(y), z='0', r='0', g='0', b='0', size='0.01')
            counter += 1
            graph.addEdge(str(counter*2-1), str(counter-2), str(counter-1))
    return counter

def build_graph(graph, root, start_ts, end_ts, node_y_table, x_count, y_count, y_name, x=0, y=0, color=None):
    if not root.childern:
        return
    r, g, b = color
    _rid = root.nid
    childs = sorted(root.childern, key=lambda x: x.ts)
    height = 0
    step = float(1)/100
    name = root.name
    if name not in node_y_table:
        node_y_table[name] = FLOAT_FORMAT % y
        y_name[FLOAT_FORMAT % y] = name
    else:
        y = float(node_y_table[name])
    width =  x + ts2x(root.ts-start_ts)
    graph.addNode(_rid, name, x=str(width), y=str(y), z=str(math.sqrt(len(childs))), r=r, g=g, b=b, size=str(math.sqrt(len(childs))))
    x_count[width] += 1
    #a bug that same y has different counts, may be caused by float accuracy
    y_count[FLOAT_FORMAT % y] += 1
    for i, child in enumerate(childs):
        _cid = child.nid
        width = x + ts2x(child.ts-start_ts)
        size = len(child.childern)
        name = child.name
        _height = None
        if name not in node_y_table:
            _height = height + y
            while FLOAT_FORMAT % _height in node_y_table.values():
                height = height + step
                _height = height + y
            node_y_table[name] = FLOAT_FORMAT % _height
            y_name[FLOAT_FORMAT % _height] = name
        else:
            _height = float(node_y_table[name])
        graph.addNode(_cid, name, x=str(width), y=FLOAT_FORMAT % _height, z=str(size), r=r, g=g, b=b, size=str(math.sqrt(size)))
        x_count[width] += 1
        y_count[FLOAT_FORMAT % _height] += 1
        _eid = _rid + _cid
        graph.addEdge(str(_eid), str(_cid), str(_rid))
        height = height - step
        build_graph(graph, child, start_ts, end_ts, node_y_table, x_count, y_count, y_name, x=x, y=_height, color=color)

class Node(object):
    def __init__(self, name, ts, nid=0, childern=None):
        self.name = name
        self.ts = ts
        self.nid = nid
        if not childern:
            childern = []
        self.childern = childern
        
    def append(self, child):
        self.childern.append(child)

def build_tree(repost_data, counter, _start_ts, _end_ts):
    if not repost_data:
        return None

    source_user = repost_data[0].strip('\n').split('\t')[2]
    end_ts = int(repost_data[0].strip('\n').split('\t')[3])
    start_ts = int(repost_data[-1].strip('\n').split('\t')[3])

    if start_ts < _start_ts:
        return None
    counter += 1
    root = Node(source_user, start_ts, nid=counter)
    repost_data.reverse()
    count = 0

    for line in repost_data:
        try:
            user, text, source_user, ts = line.strip('\n').split('\t')
            ts = int(ts)
            if not (user and text and source_user and ts):
                continue
        except ValueError:
            pass
        if ts > _end_ts:
            end_ts = ts
            break
        count += 1
        if count > MAX_COUNT:
            end_ts = ts
            break
        repost_chains = re.findall(r'//@(\S+?):', text)
        if not repost_chains:
            counter += 1
            root.append(Node(user, ts, nid=counter))
        else:
            r = root
            repost_chains.reverse()
            flag = False
            for re_user in repost_chains:
                n = None
                for child in r.childern:
                    if re_user == child.name:
                        n = child
                        break
                if not n:
                    flag = True
                    break
                r = n
            if not flag:
                counter += 1
                r.append(Node(user, ts, nid=counter))
    return root, start_ts, end_ts, count, counter

def forest_main(keyword,beg_time,end_time):
    
    _start_ts = date2ts(START_DATE)
    if beg_time < _start_ts:
        beg_time = _start_ts

    if end_time <= beg_time:
        end_time = int(time.time())

    first_start_ts, dataset = load_data(keyword,beg_time,end_time)

    height = 0
    counter = 0
    ts = []
    colors = []
    node_y_table = {}
    x_count = defaultdict(int)
    y_count = defaultdict(int)
    y_name = {}
    gexf = Gexf("Yang Han <hanyang213@gmail.com>", "Retweets evolution graph based on sail layout.")
    graph = gexf.addGraph("directed", "static", "retweets evolution graph")

    for reposts in dataset:
        if not len(reposts):
            continue
        
        root, start_ts, end_ts, count, counter = build_tree(reposts, counter, beg_time, end_time)
        _color = random_color(colors)

        build_graph(graph, root, start_ts, end_ts, node_y_table, x_count, y_count, y_name, x=ts2x(start_ts-first_start_ts), y=height, color=_color)
        height += 1

        counter = build_x_chart(graph, x_count, counter, -SEG, first_start_ts)
        counter = build_y_chart(graph, y_count, y_name, counter, -SEG)

    #print gexf.getXML()
    graph = etree.tostring(gexf.getXML(), pretty_print=True, encoding='utf-8', xml_declaration=True)

    with open('./weibo/static/gexf/forest.gexf', 'w') as gf:
        gf.write(graph)
