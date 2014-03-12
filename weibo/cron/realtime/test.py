# -*- coding: utf-8 -*-


import os
import leveldb
import zlib
import cPickle as pickle

LEVELDBPATH = '/home/mirage/leveldb'
now_datestr = '20140308'
now_leveldb_no = '20'

daily_profile_keywords_bucket = leveldb.LevelDB(os.path.join(LEVELDBPATH, './keywords/linhao_profile_keywords_%s_%s' % (now_datestr, now_leveldb_no)),
                                                    block_cache_size=8 * (2 << 25), write_buffer_size=8 * (2 << 25))

for k, v in daily_profile_keywords_bucket.RangeIter():
	  print k
	  print dict(pickle.loads(zlib.decompress(v)))
