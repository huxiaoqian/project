# -*- coding: utf-8 -*-

import os
import sys
import json
import time
from datetime import datetime
import multiprocessing
from xapian_weibo.xapian_backend import XapianSearch

stub_files = ['/home/ubuntu12/dev/data/stub/master_timeline_weibo_20130901', \
              '/home/ubuntu12/dev/data/stub/master_timeline_weibo_20130902', \
              '/home/ubuntu12/dev/data/stub/master_timeline_weibo_20130903', \
              '/home/ubuntu12/dev/data/stub/master_timeline_weibo_20130904', \
              '/home/ubuntu12/dev/data/stub/master_timeline_weibo_20130905', \
              '/home/ubuntu12/dev/data/stub/master_timeline_weibo_20130906', \
              '/home/ubuntu12/dev/data/stub/master_timeline_weibo_20130907', \
              '/home/ubuntu12/dev/data/stub/master_timeline_weibo_20130908', \
              '/home/ubuntu12/dev/data/stub/master_timeline_weibo_20130909', \
              '/home/ubuntu12/dev/data/stub/master_timeline_weibo_20130910', \
              '/home/ubuntu12/dev/data/stub/master_timeline_weibo_20130911', \
              '/home/ubuntu12/dev/data/stub/master_timeline_weibo_20130912', \
              '/home/ubuntu12/dev/data/stub/master_timeline_weibo_20130913', \
              '/home/ubuntu12/dev/data/stub/master_timeline_weibo_20130914', \
              '/home/ubuntu12/dev/data/stub/master_timeline_weibo_20130915', \
              '/home/ubuntu12/dev/data/stub/master_timeline_weibo_20130916', \
              '/home/ubuntu12/dev/data/stub/master_timeline_weibo_20130917', \
              '/home/ubuntu12/dev/data/stub/master_timeline_weibo_20130918', \
              '/home/ubuntu12/dev/data/stub/master_timeline_weibo_20130919', \
              '/home/ubuntu12/dev/data/stub/master_timeline_weibo_20130920', \
              '/home/ubuntu12/dev/data/stub/master_timeline_weibo_20130921']

stub_file_len = int(sys.argv[1])
keywords_arg = u'日本' # u'钓鱼岛' # u'九一八' # u'中国'

stub_files = stub_files[:stub_file_len]
pool_size = len(stub_files)
print 'pool_size: ', pool_size
xapian_fields = ['_id', 'user', 'retweeted_uid', 'retweeted_mid', 'text', 'timestamp', 'reposts_count', 'source', 'bmiddle_pic', 'geo', 'attitudes_count', 'comments_count', 'sentiment']#, 'terms']


def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print '%r args: %s %2.2f sec' % (method.__name__, args, te - ts)
        return result
    return timed


def test_search(stub_file):
    search_start_ts = time.time()
    xapian_weibo = XapianSearch(stub=stub_file, schema_version=5)
    count, get_results = xapian_weibo.search(query={'text': keywords_arg}, fields=xapian_fields)
    pid_num = os.getpid()
    search_end_ts = time.time()
    print "Working in Process #%d, %d, search uses %d seconds" % (pid_num, count, search_end_ts-search_start_ts)

    fw = open('./cache/%s.txt' % pid_num, 'w')
    count = 0
    tb = time.time()
    ts = tb
    for r in get_results():
        fw.write('%s\n' % json.dumps(r))

        if count % 10000 == 0:
            te = time.time()
            print 'process [%s] write speed: %s sec/per %s' % (pid_num, te - ts, 10000)
            ts = te
        count += 1
    fw.close()

    return pid_num


def mp_map():
    po = multiprocessing.Pool(pool_size)
    pool_outputs = po.map(test_search, stub_files)
    po.close()
    po.join()


def mp_map_async():
    po = multiprocessing.Pool(pool_size)
    pool_outputs = po.map_async(test_search, stub_files)
    return pool_outputs.get()


results = []
def collect_results(result):
    results.append(result)


def mp_apply_async():
    po = multiprocessing.Pool(pool_size)
    for stub_file in stub_files:
        po.apply_async(test_search, args=(stub_file, ), callback=collect_results)
    po.close()
    po.join()


@timeit
def search_multi_map_async():
    results = mp_map_async()
    count = 0
    tb = time.time()
    ts = tb
    for pid_num in results:
        print 'pid: ', pid_num
        fr = open('./cache/%s.txt' % pid_num)
        for r in fr:
            json.loads(r)

            if count % 10000 == 0:
                te = time.time()
                print '[%s] read speed: %s sec/per %s' % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), te - ts, 10000)
                ts = te
            count += 1

        os.remove('./cache/%s.txt' % pid_num)
    print count


@timeit
def search_multi():
    mp_apply_async()
    count = 0
    tb = time.time()
    ts = tb
    for pid_num in results:
        print 'pid: ', pid_num
        fr = open('./cache/%s.txt' % pid_num)
        for r in fr:
            json.loads(r)

            if count % 10000 == 0:
                te = time.time()
                print '[%s] read speed: %s sec/per %s' % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), te - ts, 10000)
                ts = te
            count += 1

        os.remove('./cache/%s.txt' % pid_num)
    print count


@timeit
def search_single():
    search_start_ts = time.time()
    xapian_weibo = XapianSearch(stub=stub_files, schema_version=5)
    count, get_results = xapian_weibo.search(query={'text': keywords_arg}, fields=xapian_fields)
    count = 0
    search_end_ts = time.time()
    print 'search single %d' % (search_end_ts-search_start_ts)
    tb = time.time()
    ts = tb
    for r in get_results():
        if count % 10000 == 0:
            te = time.time()
            print '[%s] read speed: %s sec/per %s' % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), te - ts, 10000)
            ts = te
        count += 1
    print count


if __name__ == '__main__':
    # 返回terms和text，202.45 sec, 1269341 weibos = 693812 + 575529, 4761 write speed, 30000 read speed
    # search_multi()
    
    # 返回_id：26315条每秒, 1269341 weibos, 总耗时63.78秒（检索15秒，2天）
    # 返回terms和text：5376条每秒, 1269341 weibos, 总耗时253.86秒（检索17秒，2天） 
    # 返回所有字段（包括terms）：5263条每秒, 1269341 weibos, 总耗时253.82秒（检索12秒，2天）
    # 返回所有字段（terms除外）：23255条每秒, 1269341 weibos, 总耗时67.68秒（检索13秒，2天）
    # 返回所有字段（terms除外）：
    '''
    1 天：4s, 575529 weibos, 总耗时31.15秒
    2 天：10s, 1269341 weibos, 总耗时67.68秒
    3 天：20s, 2170310 weibos, 总耗时119.22秒
    4 天：27s, 3286191 weibos, 总耗时170.84秒
    5 天：36s, 4150178 weibos, 总耗时261.86秒
    6 天：
    '''
    search_single()
    
    # 返回_id：21276条每秒 write txt, 153846条每秒 read txt, 耗时47.81秒(2天), 31.5MB
    # 返回所有字段（包括terms）：4500条每秒 write txt, 22000条每秒 read txt, 耗时219.14秒（2天）, 667 + 549.1 = 1216.1MB
    # 返回所有字段（terms除外）：16129条每秒 write txt, 37037条每秒 read txt, 耗时82.87秒（2天）, 450 + 372.6 = 822.6MB
    # 返回所有字段（terms除外）：
    '''
    2 天：max{4, 5}=5s, 1269341 weibos, 总耗时82.87秒, 822.6MB
    3 天：max{4, 5, 8}=8s, 2170310 weibos, 总耗时119.63秒, 1366MB
    4 天：max{4, 5, 9, 4}=9s, 3286191 weibos, 总耗时170.19秒, 664.8+544+450+372.6=2031.4MB
    5 天：max{6, 5, 7, 7, 4}=7s, 4150178 weibos, 总耗时195.53秒
    6 天：
    '''
    search_multi_map_async()
