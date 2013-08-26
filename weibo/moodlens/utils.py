# -*- coding: utf-8 -*-

import time, os, operator
from xapian_weibo.utils import SimpleMapReduce, count_words

def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print '%r %2.2f sec' % (method.__name__, te - ts)
        return result
    return timed

def top_keywords(get_results, top=1000):
    keywords_with_count = keywords(get_results)
    keywords_with_count.sort(key=operator.itemgetter(1))

    return keywords_with_count[len(keywords_with_count) - top:]

@timeit
def keywords(get_results):
    origin_data = []
    for r in get_results():
        origin_data.append(r['terms'].items())

    mapper = SimpleMapReduce(addcount2keywords, count_words)
    keywords_with_count = mapper(origin_data)

    return keywords_with_count

def getStopWds(filename):
    swds = set()
    f = open(filename, 'r')
    count = 0
    for line in f.readlines():
        word = line.split()[0]
        swds.add(word)
        count += 1
    print 'stop words : ', count
    return swds

swds = getStopWds('./weibo/moodlens/stpwds_linhao_20130514.txt')

def addcount2keywords(origin_keywords_with_count):
    keywords_with_count = []
    for k, v in origin_keywords_with_count:
        if k not in swds:
            keywords_with_count.append((k, v))
    return keywords_with_count

if __name__ == '__main__':
    print os.getcwd()


