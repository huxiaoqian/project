# -*- coding: utf-8 -*-

import sys
import time
import profile
import tempfile

import networkx as nx

sys.path.append('../../identify/')

from hat.fs import HadoopFS

from xapian_weibo.xapian_backend import XapianSearch

from pagerank import pagerank

statuses_search = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_weibo', schema_version=2)

users_search = XapianSearch(path='/opt/xapian_weibo/data/', name='master_timeline_user', schema_version=1)

def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print '%r args: %s %2.2f sec' % (method.__name__, args, te - ts)
        return result
    return timed

def datetime2ts(date):
    return time.mktime(time.strptime(date, '%Y-%m-%d'))

def is_in_trash_list(uid):
    return False

def acquire_status_by_id(mid):
    count, get_results = statuses_search.search(query={'_id': mid})
    # assert count==1, 'MID duplicated?'
    status = None
    for r in get_results():
        status = r
    return status

@timeit
def emulate(tmp_file, size=1000):
    g = nx.DiGraph(nx.powerlaw_cluster_graph(size, 3, 0.001))
    N = len(g.nodes())
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
    return tmp_file

@timeit
def pagerank_simluation_test(input_path):
    iter_count = 5
    top_n = 500
    job_id = 1
    sorted_ids = pagerank(job_id, iter_count, input_path, top_n)
    if sorted_ids:
        if len(sorted_ids) < 10:
            for i in range(len(sorted_ids)):
                print sorted_ids[i]
        else:
            for i in range(10):
                print sorted_ids[i]
    fs = HadoopFS()
    fs.rmr('%s/hat_results' % job_id)

@timeit
def search_topic_statuses(topic, date, window, max_size=100000):
    end_time = datetime2ts(date)
    start_time = end_time - 24*60*60*window
    query_dict = {'text': topic, 'timestamp': {'$gt': start_time, '$lt': end_time}}

    count, get_statuses_results = statuses_search.search(query=query_dict, field=['text', 'user', 'timestamp', 'retweeted_status'], max_offset=max_size)
    
    print 'topic statuses count %s' % count

    return get_statuses_results

@timeit
def load_topic_statuses(results):
    g = nx.DiGraph()    
    for status in results():
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

@timeit
def prepare_topic_statusesfor_hadoop(g):
    tmp_file = tempfile.NamedTemporaryFile(delete=False)
    N = len(g.nodes())
    print 'topic network size %s' % N
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

@timeit
def search_test(topic, join=True):
    date = '2013-5-1'
    window = 90
    max_size = 100000
    end_time = datetime2ts(date)
    start_time = end_time - 24*60*60*window
    query_dict = {'text': topic, 'timestamp': {'$gt': start_time, '$lt': end_time}}

    count, get_statuses_results = statuses_search.search(query=query_dict, field=['text', 'user', 'timestamp', 'retweeted_status'], max_offset=max_size)

    g = nx.DiGraph()  
    for status in get_statuses_results():
        try:
            if status['retweeted_status']:
                repost_uid = status['user']
                rt_mid = status['retweeted_status']
                if join:
                    source_uid = acquire_status_by_id(rt_mid)['user']
                else:
                    source_uid = 0
                g.add_edge(repost_uid, source_uid)
        except (TypeError, KeyError):
            continue

    print 'topic statuses count %s' % count

def main():
    '''话题网络 查询测试
    '''
    #测试连接查询，即查询retweeted_status项的时间
    # #hit: 18453
    # #time: 183.48s
    # search_test(topic=u'暴雨')
    # #time: 3.10s
    # search_test(topic=u'暴雨', join=False)
    # #time: 3.78s
    # search_test(topic=u'暴雨')
    # #time: 3.08s
    # search_test(topic=u'暴雨', join=False)

    # #hit: 100000
    # #time: 231.58s
    # search_test(topic=u'微博')
    # #time: 102.04s
    # search_test(topic=u'微博', join=False)
    # #time: 82.15s
    # search_test(topic=u'微博')
    # #time: 82.06s
    # search_test(topic=u'微博', join=False)

    # # #hit: 45180
    # # #time: 523.38s
    # # search_test(topic=u'篮球')
    # # #time: 5.76s
    # # search_test(topic=u'篮球', join=False)
    # # #time: 10.05s
    # # search_test(topic=u'篮球')
    # # #time: 5.46s
    # # search_test(topic=u'篮球', join=False)


    '''仿真网络 PageRank并行计算性能测试
    '''
    # #50,000 nodes pagerank
    # #time: 129.24s(multi 126.01)
    #tmp_file = tempfile.NamedTemporaryFile(delete=False)
    #tmp_file = emulate(tmp_file, size=50000)
    #input_path = tmp_file.name
    #pagerank_simluation_test(input_path)

    # #100,000 nodes pagerank
    # #time: 134.65s（multi 148.18)
    #tmp_file = tempfile.NamedTemporaryFile(delete=False)
    #tmp_file = emulate(tmp_file, size=100000)
    #input_path = tmp_file.name
    #pagerank_simluation_test(input_path)

    # #500,000 nodes pagerank
    # #time: 231.92s(multi 134.70)
    #tmp_file = tempfile.NamedTemporaryFile(delete=False)
    #tmp_file = emulate(tmp_file, size=500000)
    #input_path = tmp_file.name
    #pagerank_simluation_test(input_path)

    # #1000,000 nodes pagerank
    # #time: 358.85s(multi 168.27)
    #tmp_file = tempfile.NamedTemporaryFile(delete=False)
    #tmp_file = emulate(tmp_file, size=1000000)
    #input_path = tmp_file.name
    #pagerank_simluation_test(input_path)

    # #3000,000 nodes pagerank
    # #time: 764.14s(multi 239.23)
    #tmp_file = tempfile.NamedTemporaryFile(delete=False)
    #tmp_file = emulate(tmp_file, size=3000000)
    #input_path = tmp_file.name
    #pagerank_simluation_test(input_path)


    '''话题网络 PageRank并行计算性能测试
    '''
    # #search statuses
    # #time: 6.96s(multi 6.54)
    # #hit: 100000(multi 33940)
    #topic = u'两会'
    #date = '2013-5-1' 
    #window = 90
    #results = search_topic_statuses(topic, date, window, max_size=100000)
    # #load statuses
    # #time: 305.66s(multi 41.90)
    #g = load_topic_statuses(results)
    # #prepartion for hadoop
    # #time: 0.08s(multi 0.07)
    #input_file = prepare_topic_statusesfor_hadoop(g)
    #input_path = input_file.name
    # #mapreduce on hadoop
    # #time: 108.94s(multi 3.01)
    #pagerank_simluation_test(input_path)

    # #search statuses max_size=100000
    # #time: 43.81s(multi 10.15)
    # #hit: 100000(multi 100000)
    #topic = u'春节'
    #date = '2013-3-1'
    #window = 90
    #results = search_topic_statuses(topic, date, window, max_size=100000)
    # #load statuses
    # #time: 66.52s(multi 45.68)
    #g = load_topic_statuses(results)
    # #prepartion for hadoop
    # #time: 0.00s(multi 0.24)
    #input_file = prepare_topic_statusesfor_hadoop(g)
    #input_path = input_file.name
    # #mapreduce on hadoop
    # #time: 107.95s(multi 122.36)
    #pagerank_simluation_test(input_path)

    # #search statuses max_size=100000
    # #time: 5.39s(multi 1.89)
    # #hit: 29149(multi 10851)
    #topic = u'钓鱼岛'
    #date = '2013-5-1'
    #window = 90
    #results = search_topic_statuses(topic, date, window, max_size=100000)
    # #load statuses
    # #time: 5.14s(multi 3.80)
    #g = load_topic_statuses(results)
    # #prepartion for hadoop
    # #time: 0.03s(multi 0.03)
    #input_file = prepare_topic_statusesfor_hadoop(g)
    #input_path = input_file.name
    # #mapreduce on hadoop
    # #time: 108.19s(multi 140.67)
    #pagerank_simluation_test(input_path)

    # #search statuses max_size=100000
    # #time: 73.63s(multi 9.67)
    # #hit: 100000(multi 100000)
    #topic = u'新浪'
    #date = '2013-5-1'
    #window = 90
    #results = search_topic_statuses(topic, date, window, max_size=100000)
    # #load statuses
    # #time: 105.09s(multi 69.75)
    #g = load_topic_statuses(results)
    # #prepartion for hadoop
    # #time: 0.15s(multi 0.06)
    #input_file = prepare_topic_statusesfor_hadoop(g)
    #input_path = input_file.name
    # #mapreduce on hadoop
    # #time: 118s(multi 114.84)
    #pagerank_simluation_test(input_path)

    # #search statuses max_size=100000
    # #time: 1.43s(multi 0.41)
    # #hit: 2130(multi 796)
    #topic = u'神舟'
    #date = '2013-5-1'
    #window = 90
    #results = search_topic_statuses(topic, date, window, max_size=100000)
    # #load statuses
    # #time: 21.87s(multi 5.82)
    #g = load_topic_statuses(results)
    # #prepartion for hadoop
    # #time: 0.05s(multi 0.00)
    #input_file = prepare_topic_statusesfor_hadoop(g)
    #input_path = input_file.name
    # #mapreduce on hadoop
    # #time: 105.85s(multi )(multi 145.97)
    #pagerank_simluation_test(input_path)

    # #search statuses max_size=100000
    # #time: 21.48s(multi 4.24)
    # #hit: 76200(multi 27914)
    topic = u'黄金'
    date = '2013-5-1'
    window = 90
    results = search_topic_statuses(topic, date, window, max_size=100000)
    # #load statuses
    # #time: 548.06s(multi 89.45)
    g = load_topic_statuses(results)
    # #prepartion for hadoop
    # #time: 0.06s(multi 0.06)
    input_file = prepare_topic_statusesfor_hadoop(g)
    input_path = input_file.name
    # #mapreduce on hadoop
    # #time: 106.85s(multi 109.60)
    pagerank_simluation_test(input_path)


    '''
    性能测试结论
    数据层：
    1.从搜索引擎根据关键字检索微博的效率不够稳定，对于未曾检索过的冷数据，查询很慢，对于已经检索过的数据，由于缓存机制查询速度会大幅度提升。实际测试中，对于检索至多10万条的数据，平均需要5分钟左右。
    2.建立转发数据索引可以减少回溯的时间，直接提供网络的结构信息，进一步提高效率。
    算法层：
    1.单计算节点的PageRank并行计算在不同的模拟数据和真实数据上计算效率都很稳定。由于Hadoop框架的伸缩性很好，在多计算节点环境下会进一步提高性能，比如在三台计算节点同时工作的情况下预计2到3分钟可以处理100万节点规模的网络，可以达到计算效率的要求。
    '''

if __name__ == '__main__': main()
