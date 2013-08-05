# -*- coding: utf-8 -*-

# sort user's key-value pairs by value using Hadoop

import os
import tempfile

from datetime import datetime

from time_utils import datetime2ts, unix2hadoop_date

try:
    from hat.job import Hat
    from hat.fs import HadoopFS
except ImportError:
    print 'Hadoop module is not installed or configured.'

class UserSorter(Hat):
    def mapper(self, key, value):
        yield (value, key)

    def reducer(self, key, values):
        for value in values:
            yield (value, key)

def generate_job_id(method, date, window_size):
    date = unix2hadoop_date(datetime2ts(date))
    return 'user_%s_%s_%s' % (method, date, window_size)

def save_to_tmp(job_id, data):
    tmp_file = tempfile.NamedTemporaryFile(delete=False)
    for key, value in data.iteritems():
        tmp_file.write('%s\t%s\n' % (key, value))
    tmp_file.flush()
    fs = HadoopFS()
    fs.rmr('%s' % job_id)
    fs.mkdir('%s' % job_id)
    fs.put(tmp_file.name, '%s/hat_init' % job_id)
    return tmp_file.name

def read_from_hdfs(job_id, top_n):
    fs = HadoopFS()
    outputs = fs.cat('%s/hat_results/*' % job_id)
    if not outputs:
        return []
    if len(outputs) > top_n:
        outputs = outputs[-top_n:]
    outputs.reverse()
    sorted_uids = []
    for line in outputs:
        uid, value = line.strip().split('\t')
        sorted_uids.append(uid)
    return sorted_uids

def user_rank(user_dict, method, top_n, date, window_size):
    job_id = generate_job_id(method, date, window_size)
    input_path = save_to_tmp(job_id, user_dict)

    user_sorter = UserSorter(input_path='%s/hat_init' % job_id, output_path='%s/hat_results' % job_id)
    user_sorter.run()
    sorted_uids = read_from_hdfs(job_id, top_n)

    return sorted_uids

