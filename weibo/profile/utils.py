# -*- coding: utf-8 -*-

import time
from datetime import datetime, timedelta, date
from flask import request
from weibo.extensions import db
from weibo.model import *
from sqlalchemy import func
import json
import leveldb
import os
import operator
import re
from weibo.global_config import xapian_search_user, xapian_search_domain, fields_id, LEVELDBPATH


def datetime2ts(date):
    return time.mktime(time.strptime(date, '%Y-%m-%d'))

def ts2HMS(ts):
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts))

def ts2hour(ts):
    return int(time.strftime('%H', time.localtime(ts)))

def get_above100_weibos(topic_id, start_ts, end_ts, page):
    topic = acquire_topic_name(topic_id)
    if not topic:
        return None

    query_dict = {
        'timestamp': {
            '$gt': start_ts, 
            '$lt': end_ts
        }, 
        '$or': [],
        'reposts_count': {
            '$gt': 100
        }
    }
    query_dict['$or'].append({'text': [topic]})
    fields_list = ['_id','text', 'user', 'timestamp', 'reposts_count', 'comments_count', 'attitudes_count', 'retweeted_status']
    count, get_results = statuses_search.search(query=query_dict, sort_by=['reposts_count'], field=fields_list)

    statuses = []
    for r in get_results():
        id = r['_id']
        text = r['text']
        user = r['user']
        timestamp = r['timestamp']
        reposts_count = r['reposts_count']
        comments_count = r['comments_count']
        attitudes_count = r['attitudes_count']
        try:
            retweeted_mid = r['retweeted_status']
        except KeyError:
            retweeted_mid = None
        rt = None
        if retweeted_mid:
            retweeted_status = getWeiboInfoByMid(retweeted_mid)
            if retweeted_status:
                rt = {'text': retweeted_status['text'], 'mid': retweeted_mid}
        weibo_url = weiboinfo2url(user, id)
        statuses.append({'mid': id, 'text': text, 'uid': user, 'timestamp': timestamp, 'weibo_url': weibo_url, \
            'reposts_count': reposts_count, 'comments_count': comments_count, 'attitudes_count': attitudes_count, \
            'rt': rt})
    startoff = 0 + (page-1) * 10
    endoff = startoff + 9
    pages = len(statuses) / 10
    return statuses[startoff:endoff], pages


def weiboinfo2url(uid, mid):
    return "http://weibo.com/{uid}/{mid}".format(uid=uid, mid=mid_to_str(mid))


def mid_to_str(mid):
    mid = str(mid)
    id1 = mid[0: 2]
    id2 = mid[2: 9]
    id3 = mid[9: 16]
    id_list = [id1, id2, id3]
    id_list = [base62_encode(int(mid)) for mid in id_list]
    return "".join(map(str, id_list))

ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"


def base62_encode(num, alphabet=ALPHABET):
    """Encode a number in Base X

    `num`: The number to encode
    `alphabet`: The alphabet to use for encoding
    """
    if (num == 0):
        return alphabet[0]
    arr = []
    base = len(alphabet)
    while num:
        rem = num % base
        num = num // base
        arr.append(alphabet[rem])
    arr.reverse()
    return ''.join(arr)

def make_network_graph(current_date, topic_id, window_size, key_user_labeled=True):
    date = current_date

    if key_user_labeled:
        key_users = read_key_users(current_date, window_size, topic_id, top_n=10)
    else:
        key_users = []

    topic = acquire_topic_name(topic_id)
    if not topic:
        return None
              
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
            _node = graph.addNode(node_id[node], str(node), x=str(x), y=str(y), z='0', r='255', g='51', b='51', size=str(math.sqrt(10*degree)))
        else:
            _node = graph.addNode(node_id[node], str(node), x=str(x), y=str(y), z='0', r='0', g='204', b='204', size=str(math.sqrt(8*degree)))
        user_info = acquire_user_by_id(uid)
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

def acquire_topic_id(name):
    item = db.session.query(Topic).filter_by(topicName=name).first()
    if not item:
        #create a topic
        item = Topic(topicName=name)
        db.session.add(item)
        db.session.commit()
        db.session.refresh(item)
    return item.id
def read_rank_results(top_n, identifyRange, method, date, window, topic_id=None):
    data = []
    if identifyRange == 'area':
        items = db.session.query(AreaUserIdentification).filter_by(topicId=topic_id, identifyMethod=method, identifyWindow=window, identifyDate=date).order_by(AreaUserIdentification.rank.asc()).limit(top_n)
    else:
        return data
    if items.count():
        for item in items:
            rank = item.rank
            uid = item.userId
            user = acquire_user_by_id(uid)
            if not user:
                continue
            name = user['name']
            location = user['location']
            fol_count = user['fol_count']
            fri_count = user['fri_count']
            status_count = user['status_count']
            if topic_id == 1:
                if uid in inside_sea:
                    row = (rank, uid, name, location, fol_count, fri_count, status_count, 'inside')
                else:
                    row = (rank, uid, name, location, fol_count, fri_count, status_count, 'outside')
            else:
                row = (rank, uid, name, location, fol_count, fri_count, status_count)
            data.append(row)
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
def is_in_black_list(uid):
    return False

def get_leveldb(method, ts):
    date = datetime.fromtimestamp(ts)
    db_name = 'hanyang_daily_user_%s_' % method
    db_name += str(date.year)
    if date.month < 10:
        db_name += '0%s' % str(date.month)
    else:
        db_name += str(date.month)
    if date.day < 10:
        db_name += '0%s' % str(date.day)
    else:
        db_name += str(date.day)
    return db_name

def active_rank(top_n, date, window_size):
    date_time = datetime2ts(date)
    uid_active = {}
    if window_size == 1:
        db_name = get_leveldb('active', date_time)
        daily_user_active_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, db_name),
                                                   block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
        
        for uid, active in daily_user_active_bucket.RangeIter():
            uid = int(uid)
            active = float(active)
            uid_active[uid] = active
    else:
        for i in range(window_size):
            db_name = get_leveldb('active', date_time - i*24*60*60)
            daily_user_active_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, db_name),
                                              block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))
            for uid, active in daily_user_active_bucket.RangeIter():
                uid = int(uid)
                active = float(active)
                if uid not in uid_active:
                    uid_active[uid] = 0
                uid_active[uid] += active

    sorted_uid_active = sorted(uid_active.iteritems(), key=operator.itemgetter(1), reverse=True)
                
    sorted_uids = []
    count = 0
    for uid, value in sorted_uid_active:
        if is_in_black_list(uid):
            continue
        if count >= top_n:
            break
        sorted_uids.append(uid)
        count += 1

    data = sorted_uids
    return data

def getFieldUsersByScores(fieldName, start_offset, end_offset, update_date='20130430'):
    if fieldName == 'oversea':
        count, get_results = xapian_search_user.search(query={'location': '海外'}, sort_by=['followers_count'], max_offset=10000, fields=['_id'])
        fields_list = []
        for r in get_results():
            fields_list.append(r['_id'])
        return fields_list[start_offset:end_offset]
    query_dict = {
        'domain': str(fields_id[fieldName]),
        'followers_count': {'$gt': 1000}
    }
    count, get_results = xapian_search_domain.search(query=query_dict, sort_by =['-followers_count'], fields=['_id'], max_offset=10000)
    fields_list = [user['_id'] for user in get_results()]
    return fields_list[start_offset:end_offset]

def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print '%r %2.2f sec' % (method.__name__, te-ts)
        return result
    return timed

@timeit
def store2riak(get_results):
    count = 0
    for r in get_results():
        # 微博是否为转发微博
        weibo_is_retweet_status_bucket = client.bucket('lijun_weibo_is_retweet_status')
        is_retweet_status_bucket = 1 if weibo['retweeted_status'] else 0
        new_node = weibo_is_retweet_status_bucket.new(weibo['id'], data=is_retweet_status_bucket)
        new_node.store()
        count += 1

    print 'total store count:', count

@timeit
def test_riak_write(test_bucket, n):
    for i in range(n):
        test_bucket.new(str(i), data=i).store()

@timeit
def test_riak_read(test_bucket, n):
    for i in range(n):
        r = test_bucket.get(str(i))
        # stable version
        # data = r.get_data()
        # master_version
        data = r.data

        if data != i:
            raise
            
def local2datetime(time_str):
    time_format = '%a %b %d %H:%M:%S +0800 %Y'
    return datetime.fromtimestamp(int(time.mktime(time.strptime(time_str, time_format))))

def ts2datetime(ts):
    return time.strftime('%Y-%m-%d', time.localtime(ts))



def ts2date(ts):
    return date.fromtimestamp(int(float(ts)))

def time2ts(date):
    return time.mktime(time.strptime(date, '%Y-%m-%d'))

def hot_uid_by_word(starttime, endtime, count=50):
    '''筛选出词语表中有超过50条记录的博主
    '''
    startdate = ts2datetime(time2ts(starttime))
    enddate =  ts2datetime(time2ts(endtime))
    uids = set()
    uids_count = db.session.query(Words.uid, func.count(Words.id)).\
                               filter(Words.postDate>startdate, Words.postDate<enddate).\
                               group_by(Words.uid).\
                               having(func.count(Words.id) > count).all()
    for uid, count in uids_count:
        uids.add(uid)
    return uids

def last_week(weeks=1, interval=1):
    '''计算当前日期的上一周起止日期（从上周一到本周一）
        weeks为从本周往前回溯的周数
        interval为间隔的周数
    '''
    now_weekday = datetime.now().weekday()
    now_date = date.today()
    this_monday = now_date - timedelta(days=now_weekday) - timedelta(days=7*(weeks-1))
    last_monday = this_monday - timedelta(days=7*interval)
    return last_monday.isoformat(), this_monday.isoformat()

def last_month(mon_num=1):
    '''计算当前日期回溯若干月
    '''
    now_date = date.today()
    last_date = now_date - timedelta(days=30 * mon_num)
    return last_date.isoformat(), now_date.isoformat()

def last_day(day_num=1):
    now_date = date.today()
    last_date = now_date - timedelta(days=day_num)
    return last_date.isoformat(), now_date.isoformat()

def emoticon_find(text):
    seed_set = get_official_seed_set()
    emotion_pattern = r'\[(\S+?)\]'
    remotions = re.findall(emotion_pattern, text)

    emoticons = []
    if remotions:
        for e in remotions:
            if e in seed_set:
                emoticons.append(e.decode('utf-8'))

    return emoticons

def get_official_seed_set():
    seed_set = set([])
    with open('weibo/profile/data/official_emoticons.txt') as f:
        for l in f:
            seed_set.add(l.rstrip())
    return seed_set

def main():
    #last_week()
    getFieldUsersByScores('finance', 0, 19)
    pass
    
if __name__ == '__main__':
    print last_day(1)
            
