# -*- coding: utf-8 -*-

import os
import multiprocessing
from xapian_weibo.xapian_backend import XapianSearch

stub_files = ['/home/ubuntu12/dev/data/stub/master_timeline_weibo_20130901', \
              '/home/ubuntu12/dev/data/stub/master_timeline_weibo_20130902']

def test_search(stub_file):
    xapian_weibo = XapianSearch(stub=stub_file, schema_version=5, multi_processing=False)
    count, get_results = xapian_weibo.search(query={'text': u'中国'}, fields=['text', 'terms'])
    print "Working in Process #%d, %d" % (os.getpid(), count)
    yield get_results


def mp_map():
    po = multiprocessing.Pool(8)
    pool_outputs = po.map(test_search, stub_files)
    print pool_outputs
    po.close()
    po.join()


def mp_map_async():
    po = multiprocessing.Pool(8)
    pool_outputs = po.map_async(test_search, stub_files)
    print pool_outputs.get()


results = []
def collect_results(result):
    results.append(result)


def mp_apply_async():
    po = multiprocessing.Pool(8)
    for stub_file in stub_files:
        po.apply_async(test_search, args=(stub_file, ), callback=collect_results)
    po.close()
    po.join()
    for generator in results:
        for r in generator():
        	  print r['terms']


if __name__ == '__main__':
    # mp_map()

    # mp_map_async()

    mp_map_async()
