# -*- coding: utf-8 -*-

import time

try:
    from hat.fs import HadoopFS
except ImportError:
    print 'Hadoop module is not installed.'

def unix2date(ts):
    return time.strftime('%Y_%m_%d', time.localtime(ts))

def generate_job_id(field_id, topic_id):
    date = unix2date(time.time())
    job_id = '%s_%s_%s' % (date, field_id, topic_id)
    return job_id

def monitor(job_id):
    fs = HadoopFS()
    finished = False
    has_tmps = False
    outputs = fs.ls('%s' % job_id)
    if not outputs:
        return 'data_not_prepared'
    count = 0
    for line in outputs:
        if 'tmp' in line:
            count += 1
            has_tmps = True
        if 'results' in line:
             if not has_tmps:
                 finished = True
    if not finished:
        return 'stage%s' % count
    else:
        return 'finished'

def read_hadoop_results(job_id, top_n, page_num):
    data = []
    fs = HadoopFS()
    outputs = fs.cat('%s/hat_results/*' % job_id)
    if not outputs:
        return 'results_not_prepared'
    if len(outputs) > top_n:
        outputs = outputs[-top_n:]
    outputs.reverse()
    rank = 1
    for line in outputs:
        name, pr = line.strip().split('\t')
        uid = name
        location = u'北京'
        followers = 10000
        friends = 100
        comparison = 1
        status = 1
        row = (rank, uid, name, location, followers, friends, comparison, status)
        data.append(row)
        rank += 1
    return data

def main():
    job_id = '2013_04_11_-1_1'
    print monitor(job_id)
    data = read_results(job_id, 50, 10)
    print data

if __name__ == '__main__': main()
