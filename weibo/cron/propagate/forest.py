#-*- coding: utf-8 -*-

import os
import re
import time
import datetime
import codecs
import random
import math
import sys

from collections import defaultdict

from gexf import Gexf
from lxml import etree
from xapian_weibo.xapian_backend import XapianSearch
from global_config import xapian_search_user
from get_result import *
from pyelevator import WriteBatch, Elevator

MAX_COUNT = 15000
START_DATE = '2013-9-1'
END_DATE = '2013-9-5'
FLOAT_FORMAT = '%.2f'
SEG = 2
weibo_fields = ['_id', 'user', 'retweeted_uid', 'retweeted_mid', 'text', 'timestamp', \
                'reposts_count', 'source', 'bmiddle_pic', 'geo', 'attitudes_count', \
                'comments_count', 'sentiment']
user_fields = ['_id', 'province', 'city', 'verified', 'name', 'friends_count', \
               'gender', 'profile_image_url', 'verified_type', 'followers_count', \
               'followers', 'location', 'statuses_count', 'friends', 'description', \
               'created_at']

try:
    from weibo.global_config import LEVELDBPATH
except:
    LEVELDBPATH = '/media/data/leveldb/'
    print 'not in web environment'

def _default_elevator(db_name='default'):
    db = Elevator(db_name, transport='tcp', endpoint='192.168.2.31:4141')
    return db


def init_db():
    E = _default_elevator()
    E.createdb(os.path.join(LEVELDBPATH, 'linhao_weibo_gexf_forest'))

    E.disconnect()


def save_weibo_tree(_id, whole_g):
    E = _default_elevator(os.path.join(LEVELDBPATH, 'linhao_weibo_gexf_forest'))
    E.Put(str(_id), whole_g)
    E.disconnect()
#    print _id, whole_g

def get_weibo_tree(_id):
    E = _default_elevator(os.path.join(LEVELDBPATH, 'linhao_weibo_gexf_forest'))
    v = E.Get(str(_id))
    E.disconnect()
    return v
#    print _id, whole_g

def ts2datetimestr(ts):
    return time.strftime('%Y%m%d', time.localtime(ts))

def date2ts(date):
    return int(time.mktime(time.strptime(date, '%Y-%m-%d')))

def unix2local(ts):
    return time.strftime('%Y-%m-%d %H:00', time.localtime(ts))

def getXapianWeiboByDuration(datestr_list):
    path = '/home/ubuntu12/dev/data/stub/master_timeline_weibo_'
    stub_file_list = []

    for datestr in datestr_list:
        stub_file = path + datestr
        if os.path.exists(stub_file):
            stub_file_list.append(stub_file)

    if len(stub_file_list):
        xapian_search_weibo = XapianSearch(stub=stub_file_list, include_remote=True, schema_version=5)
        return xapian_search_weibo 
    else:
        return None

def target_whole_xapian_weibo():
    datestr = '20130904'
    during = 4

    ts = int(time.mktime(time.strptime(datestr, '%Y%m%d')))
    datelist = []

    for i in range(0, during):
        now_date = time.strftime('%Y%m%d', time.localtime(ts - i * 24 * 3600))
        datelist.append(now_date)
        
    xapian_weibo = getXapianWeiboByDuration(datelist)
    return xapian_weibo

def target_whole_xapian_user():
    XAPIAN_USER_DATA_PATH = '/media/data/'
    xapian_search_user = XapianSearch(path=XAPIAN_USER_DATA_PATH, name='master_timeline_user', schema_version=1)
    
    return xapian_search_user

whole_xapian_weibo = target_whole_xapian_weibo()
whole_xapian_user = target_whole_xapian_user()

def getNone():
    user = dict()
    user['id'] = 0
    user['province'] = 'None'
    user['bi_followers_count'] = 'None'
    user['verified'] = 'None'
    user['description'] = 'None'
    user['friends_count'] = 0
    user['city'] = 'None'
    user['gender']  = 'None'
    user['profile_image_url'] = '#'
    user['verified_reason'] = 'None'
    user['followers_count'] = 0
    user['location'] = 'None'
    user['statuses_count'] = 0
    user['name'] = 'None'
    return user

def getWeiboByMid(mid):
    weibo = whole_xapian_weibo.search_by_id(int(mid), fields=weibo_fields)
    status = {}
    if weibo:
        for field in weibo_fields:
            if field == 'user':
                status['user'] = getUserByUid(weibo['user'])
            elif field == 'timestamp':
                status['created_at'] = time.strftime("%a %b %d %H:%M:%S +0800 %Y", time.localtime(weibo['timestamp']))
            else:
                status[field] = weibo[field]
    else:
        for field in weibo_fields:
            if field == 'user':
                status['user'] = getNone()
            elif field == 'timestamp':
                status['created_at'] = time.strftime("%a %b %d %H:%M:%S +0800 %Y", time.localtime(1377964800))
            else:
                status[field] = 'None'
    status['id'] = int(mid)
    status['mid'] = str(mid)

    return status

def getUserByUid(uid):
    user = whole_xapian_user.search_by_id(int(uid), fields=user_fields)
    user_dict = {}
    if user:
        for field in user_fields:
            user_dict[field] = _utf_8_decode(user[field])
    else:
        user_dict['name'] = str(uid)
        user_dict['location'] = u'未知'
        user_dict['profile_image_url'] = u''

    result_dict = {}
    for k, v in user_dict.iteritems():
        result_dict[k] = v
    result_dict['id'] = int(uid)

    return result_dict

def _utf_8_decode(stri):
    if isinstance(stri, str):
        return unicode(stri, 'utf-8')
    return stri

def load_data(keyword,topic_id):
    dataset = []
    blog_rel_list = readPropagateWeibo(topic_id)

    n = 0
    ts = []
    for status in blog_rel_list:
        print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())), status['status']['_id']                
        weibo = getWeiboByMid(status['status']['_id'])
        retweeted_mid = weibo['retweeted_mid']
        source_weibo = weibo
        if retweeted_mid != 0:
            source_weibo = getWeiboByMid(retweeted_mid)

        number,source = whole_xapian_weibo.search(query={'retweeted_mid': source_weibo['id']}, sort_by=['timestamp'],max_offset=5000)#查找热门微博的转发微博
        print number
        if not number:
            n = n + 1
            continue

        reposts = []#存储转发微博的信息
        for sw in source():
            number,get_users = xapian_search_user.search(query={'_id': sw['user']}, fields=['name'])
            ts.append(int(sw['timestamp']))
            for user in get_users():
                line = user['name'] + '\t'+ sw['text'] + '\t' + source_weibo['user']['name'] +'\t'+ str(int(sw['timestamp']))
                reposts.append(line)
        dataset.append(reposts)
    if n == len(blog_rel_list):
        flag = 0
    else:
        flag = 1
    return min(ts), dataset , flag

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

def build_tree(repost_data, counter, start=START_DATE, end=END_DATE):
    if not repost_data:
        return None

    _start_ts = date2ts(start)
    _end_ts = date2ts(end)
    
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

def forest_main(keyword,topic_id):

    first_start_ts, dataset, flag = load_data(keyword,topic_id)

    if flag == 0:
        return 0

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

    n = 0
    for reposts in dataset:
        if not len(reposts):
            continue
        
        root, start_ts, end_ts, count, counter = build_tree(reposts, counter)
        _color = random_color(colors)

        build_graph(graph, root, start_ts, end_ts, node_y_table, x_count, y_count, y_name, x=ts2x(start_ts-first_start_ts), y=height, color=_color)
        height += 1

        counter = build_x_chart(graph, x_count, counter, -SEG, first_start_ts)
        counter = build_y_chart(graph, y_count, y_name, counter, -SEG)

        n = n + 1
        print n

    graph = etree.tostring(gexf.getXML(), pretty_print=False, encoding='utf-8', xml_declaration=True)

    save_weibo_tree(str(topic_id), graph)

    return 1

if __name__ == "__main__":

##    init_db()
    v = get_weibo_tree('11')
    print v
##    result = forest_main('开学',11)
##    print result
