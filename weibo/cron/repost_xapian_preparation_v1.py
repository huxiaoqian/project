# -*- coding: utf-8 -*-

import leveldb
import json
import os
import time
from csv2json import itemLine2Dict


LEVELDBPATH = '/home/mirage/leveldb'

weibo_repost_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, 'linhao_global_weibo_repost_20131210'),
                                      block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))


def csv2reposts():
    path = './csv/'
    files = os.listdir(path)
    print 'files count: ', len(files)
    count = 0
    ts = te = time.time()
    for fname in files:
        f = open(path + fname, 'r')
        for line in f:
            count += 1
            if count % 10000 == 0:
                te = time.time()
                print count, '%s sec' % (te - ts)
                ts = te

            itemdict = itemLine2Dict(line)

            if itemdict and itemdict['retweeted_mid'] and itemdict['retweeted_mid'] != '':
            	retweeted_mid = itemdict['retweeted_mid']
                retweeted_uid = itemdict['retweeted_uid']
                mid = itemdict['_id']
                user = itemdict['user']
                timestamp = itemdict['timestamp']
                key = str(retweeted_mid) 
                try:
            	    reposts = json.loads(weibo_repost_bucket.Get(key))
                    reposts.extend([str(mid) + '_' + str(user) + '_' + str(timestamp)])
                except KeyError:
                    reposts = [str(mid) + '_' + str(user) + '_' + str(timestamp)]

                weibo_repost_bucket.Put(key, json.dumps(reposts))

        f.close()


if __name__ == '__main__':
    csv2reposts()